package com.earnix.parquet.columnar.reader;

import com.earnix.parquet.columnar.RowGroupRowIndex;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.earnix.parquet.columnar.reader.ParquetReaderUtils.getLen;
import static com.earnix.parquet.columnar.reader.ParquetReaderUtils.getStartOffset;
import static com.earnix.parquet.columnar.reader.ParquetReaderUtils.readInMemChunk;

/**
 * A {@link com.earnix.parquet.columnar.reader.ParquetColumnarReader} that reads from files
 */
public class IndexedParquetColumnarReaderImpl implements IndexedParquetColumnarReader
{
	private final MessageType messageType;
	private final ParquetReaderInputStreamSupplier inputStreamSupplier;
	private final FileMetaData fileMetaData;
	private final RowGroupRowIndex rowGroupRowIndex;
	private final List<ColumnDescriptor> columnDescriptors;
	Map<List<String>, ColumnDescriptor> descriptorByPathMap;
	List<Map<ColumnDescriptor, ColumnChunk>> rowGroupToIndexedMetadata;

	/**
	 * Construct an index parquet columnar file reader
	 *
	 * @param inputStreamSupplier supplier to get arbitrary InputStreams for parquet files agnostic to backing storage
	 *                            (s3, filesystem, memory, etc.)
	 * @throws IOException on an IO failure reading the file
	 */
	public IndexedParquetColumnarReaderImpl(ParquetReaderInputStreamSupplier inputStreamSupplier) throws IOException
	{
		this.inputStreamSupplier = inputStreamSupplier;
		this.fileMetaData = inputStreamSupplier.readMetaData();
		this.rowGroupRowIndex = new RowGroupRowIndex(fileMetaData);
		this.messageType = ParquetMetadataUtils.buildMessageType(fileMetaData);

		rowGroupToIndexedMetadata = new ArrayList<>(fileMetaData.getRow_groupsSize());

		for (RowGroup rowGroup : fileMetaData.getRow_groups())
		{
			Map<ColumnDescriptor, ColumnChunk> columnDescriptorColumnMetaDataMap = new HashMap<>();
			Iterator<ColumnChunk> it = rowGroup.getColumnsIterator();
			while (it.hasNext())
			{
				ColumnChunk columnChunk = it.next();
				ColumnDescriptor descriptor = messageType.getColumnDescription(
						columnChunk.getMeta_data().getPath_in_schema().toArray(new String[0]));
				ColumnChunk old = columnDescriptorColumnMetaDataMap.put(descriptor, columnChunk);
				assertNoDuplicateColumnChunks(old, descriptor);
			}
			rowGroupToIndexedMetadata.add(columnDescriptorColumnMetaDataMap);
		}

		descriptorByPathMap = new HashMap<>();
		this.columnDescriptors = List.copyOf(messageType.getColumns());
		for (ColumnDescriptor descriptor : this.columnDescriptors)
		{
			descriptorByPathMap.put(Arrays.asList(descriptor.getPath()), descriptor);
		}
	}

	private void assertNoDuplicateColumnChunks(ColumnChunk old, ColumnDescriptor descriptor)
			throws UnsupportedEncodingException
	{
		if (old != null)
		{
			throw new UnsupportedEncodingException(
					"Col " + descriptor + "present twice in RowGroup " + rowGroupToIndexedMetadata.size());
		}
	}

	@Override
	public ColumnChunk getColumnChunk(int rowGroup, ColumnDescriptor descriptor)
	{
		return getColumnChunk(rowGroup, descriptor, true);
	}

	private ColumnChunk getColumnChunk(int rowGroup, ColumnDescriptor descriptor, boolean deepCopy)
	{
		if (rowGroup >= this.rowGroupToIndexedMetadata.size())
		{
			throw new IllegalArgumentException("Tried to read row group " + rowGroup + " but there are only "
					+ this.rowGroupToIndexedMetadata.size() + " row groups.");
		}
		Map<ColumnDescriptor, ColumnChunk> rowGrpMap = this.rowGroupToIndexedMetadata.get(rowGroup);
		ColumnChunk colChunk = rowGrpMap.get(Objects.requireNonNull(descriptor, "descriptor must not be null"));
		ColumnChunk ret = Objects.requireNonNull(colChunk, "Column not found in row group.");
		return deepCopy ? ret.deepCopy() : ret;
	}

	@Override
	public Pair<ColumnChunk, IOSupplier<InputStream>> getInputStreamSupplier(int rowGroup, ColumnDescriptor descriptor)
	{
		return getInputStreamSupplier(rowGroup, descriptor, true);
	}

	private Pair<ColumnChunk, IOSupplier<InputStream>> getInputStreamSupplier(int rowGroup, ColumnDescriptor descriptor,
			boolean deepCopy)
	{
		ColumnChunk columnChunk = getColumnChunk(rowGroup, descriptor, deepCopy);
		IOSupplier<InputStream> fileRangeInputStreamSupplier = getFileRangeInputStreamSupplier(columnChunk);
		return new ImmutablePair<>(columnChunk, fileRangeInputStreamSupplier);
	}

	private IOSupplier<InputStream> getFileRangeInputStreamSupplier(ColumnChunk columnChunk)
	{
		long start = getStartOffset(columnChunk);
		long len = getLen(columnChunk);

		return () -> inputStreamSupplier.createInputStream(start, len);
	}

	@Override
	public InMemChunk readInMem(int rowGroup, ColumnDescriptor descriptor) throws IOException
	{
		Pair<ColumnChunk, IOSupplier<InputStream>> chunk = getInputStreamSupplier(rowGroup, descriptor, false);
		try (InputStream is = chunk.getRight().get())
		{
			InMemChunk inMemChunk = readInMemChunk(descriptor, is, getLen(chunk.getLeft()),
					chunk.getLeft().getMeta_data().getCodec());
			return inMemChunk;
		}
	}

	@Override
	public ColumnDescriptor getDescriptorByPath(String... path)
	{
		return descriptorByPathMap.get(Arrays.asList(path));
	}

	@Override
	public int getNumRowGroups()
	{
		return fileMetaData.getRow_groupsSize();
	}

	@Override
	public long getNumRowsInRowGroup(int rowGroup)
	{
		return fileMetaData.getRow_groups().get(rowGroup).getNum_rows();
	}

	@Override
	public long getTotalNumRows()
	{
		return fileMetaData.getNum_rows();
	}

	@Override
	public RowGroupRowIndex getRowGroupRowIndex()
	{
		return rowGroupRowIndex;
	}

	@Override
	public MessageType getMessageType()
	{
		return messageType;
	}

	@Override
	public List<ColumnDescriptor> getColumnDescriptors()
	{
		return columnDescriptors;
	}

	@Override
	public ColumnDescriptor getDescriptor(int colOffset)
	{
		return columnDescriptors.get(colOffset);
	}

	@Override
	public List<KeyValue> getKeyValueFileMetadata()
	{
		return ParquetMetadataUtils.deepCopyKeyValueMetadata(fileMetaData.getKey_value_metadata());
	}
}
