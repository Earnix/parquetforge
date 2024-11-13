package com.earnix.parquet.columnar.reader;

import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A parquet columnar file reader that indexes columns and row groups to allow reading arbitrary columns and row groups
 * by raw input streams, and into {@link InMemChunk}. Also supports fetching ColumnChunk metadata data from for
 * arbitrary columns without reading the column pages.<br> All lookups are done by maps that are constructed when this
 * reader is built to ensure O(1) performance regardless of how many columns and row groups are present.
 */
public class IndexedParquetColumnarFileReader extends ParquetColumnarFileReader
{
	List<Map<ColumnDescriptor, ColumnChunk>> rowGroupToIndexedMetadata;

	/**
	 * Construct an index parquet columnar file reader
	 *
	 * @param parquetFilePath the path to the parquet file.
	 * @throws IOException on an IO failure reading the file
	 */
	public IndexedParquetColumnarFileReader(Path parquetFilePath) throws IOException
	{
		super(parquetFilePath);
		FileMetaData fileMetaData = readMetaData();

		rowGroupToIndexedMetadata = new ArrayList<>(fileMetaData.getRow_groupsSize());

		for (RowGroup rowGroup : fileMetaData.getRow_groups())
		{
			Map<ColumnDescriptor, ColumnChunk> columnDescriptorColumnMetaDataMap = new HashMap<>();
			Iterator<ColumnChunk> it = rowGroup.getColumnsIterator();
			while (it.hasNext())
			{
				ColumnChunk columnChunk = it.next();
				ColumnDescriptor descriptor = buildDescriptor(columnChunk.getMeta_data());
				ColumnChunk old = columnDescriptorColumnMetaDataMap.put(descriptor, columnChunk);
				assertNoDuplicateColumnChunks(old, descriptor);
			}
			rowGroupToIndexedMetadata.add(columnDescriptorColumnMetaDataMap);
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

	/**
	 * Fetch the footer metadata for the specified column. Note: This returns a COPY of the metadata to prevent the
	 * ColumnChunk thrift object from being inadvertently modified. Avoid calling this within a loop.
	 *
	 * @param rowGroup   the row group number
	 * @param descriptor the column descriptor
	 * @return the ColumnChunk metadata present within the footer metadata
	 */
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

	/**
	 * Get an input stream supplier for a specific column chunk. This function returns {@link ColumnChunk} too because
	 * it is already looked up, and it is necessary to read the column because of the need to know what
	 * {@link org.apache.parquet.format.CompressionCodec} is used. Note that this is a deep copy of {@link ColumnChunk}
	 * and as such this should not be called in a loop<br>
	 * <p>
	 * An input stream supplier is returned rather than an input stream because there are use cases for needing to
	 * construct multiple input streams for the same column chunk, such as when uploading to an object storage and
	 * needing to retry due to a network interruption.
	 * </p>
	 *
	 * @param rowGroup   the row group offset
	 * @param descriptor the column descriptor
	 * @return the column chunk and input stream supplier
	 */
	public Pair<ColumnChunk, FileRangeInputStreamSupplier> getInputStreamSupplier(int rowGroup,
			ColumnDescriptor descriptor)
	{
		return getInputStreamSupplier(rowGroup, descriptor, true);
	}

	private Pair<ColumnChunk, FileRangeInputStreamSupplier> getInputStreamSupplier(int rowGroup,
			ColumnDescriptor descriptor, boolean deepCopy)
	{
		ColumnChunk columnChunk = getColumnChunk(rowGroup, descriptor, deepCopy);
		FileRangeInputStreamSupplier fileRangeInputStreamSupplier = getFileRangeInputStreamSupplier(columnChunk);
		return new ImmutablePair<>(columnChunk, fileRangeInputStreamSupplier);
	}

	private FileRangeInputStreamSupplier getFileRangeInputStreamSupplier(ColumnChunk columnChunk)
	{
		long start = getStartOffset(columnChunk);
		long len = getLen(columnChunk);

		return new FileRangeInputStreamSupplier(this.getParquetFilePath(), start, len);
	}

	/**
	 * Read a specific column chunk in a row group into memory
	 *
	 * @param rowGroup   the row group offset
	 * @param descriptor the column to read into memory
	 * @return the in memory chunk.
	 * @throws IOException on failure to read in the file
	 */
	public InMemChunk readInMem(int rowGroup, ColumnDescriptor descriptor) throws IOException
	{
		Pair<ColumnChunk, FileRangeInputStreamSupplier> chunk = getInputStreamSupplier(rowGroup, descriptor, false);
		InMemChunk inMemChunk = readInMemChunk(descriptor, chunk.getRight().get(), chunk.getRight().getLen(),
				chunk.getLeft().getMeta_data().getCodec());
		return inMemChunk;
	}
}
