package com.earnix.parquet.columnar;

import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.DoubleSupplier;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnWriteStoreV1;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.schema.MessageType;

import com.earnix.parquet.columnar.compressors.CompressorZstdImpl;
import com.earnix.parquet.columnar.page.InMemPageWriter;
import org.apache.parquet.schema.PrimitiveType;

public class RowGroupColumnarWriterImpl implements RowGroupColumnarWriter
{
	private final MessageType messageType;
	private final ParquetProperties parquetProperties;

	private final ConcurrentMap<ColumnDescriptor, InMemPageWriter> pageWriters = new ConcurrentHashMap<>();

	/**
	 * Number of rows in this row group
	 */
	private final int numRows;

	public RowGroupColumnarWriterImpl(MessageType messageType, ParquetProperties parquetProperties, int numRows)
	{
		this.messageType = messageType;
		this.parquetProperties = parquetProperties;
		this.numRows = numRows;
		CompressionCodecFactory.BytesInputCompressor compressor = new CompressorZstdImpl();
	}

	@Override
	public void writeColumn(String columnName, double[] vals)
	{
		if (vals.length != numRows)
			throw new IllegalArgumentException();
		writeColumn(columnName, new DoubleSupplier()
		{
			int idx = 0;

			@Override
			public double getAsDouble()
			{
				return vals[idx++];
			}
		});
	}

	public void writeColumn(String columnName, DoubleSupplier doubleSupplier)
	{
		PageWriteStore pageWriteStore = path -> pageWriters.computeIfAbsent(path, k -> new InMemPageWriter());
		try (ColumnWriteStore writeStore = new ColumnWriteStoreV1(messageType, pageWriteStore, parquetProperties))
		{
			ColumnDescriptor path = new ColumnDescriptor(new String[] { columnName },
					(PrimitiveType) messageType.getType(columnName), 1, 0);
			try (ColumnWriter columnWriter = writeStore.getColumnWriter(path))
			{
				for (int i = 0; i < numRows; i++)
				{
					columnWriter.write(doubleSupplier.getAsDouble(), 0, 0);
					writeStore.endRecord();
				}
				writeStore.flush();
			}
		}
	}

	@Override
	public void writeColumn(String columnName, int[] vals)
	{

	}

	@Override
	public void writeColumn(String columnName, long[] vals)
	{

	}

	@Override
	public void writeColumn(String columnName, String[] vals)
	{

	}

	@Override
	public void finishGroup()
	{
		System.out.println(bytesToPersist());
	}

	public long bytesToPersist()
	{
		return this.pageWriters.values().stream().mapToLong(InMemPageWriter::allocatedSize).sum();
	}

	private List<ColumnChunk> writeGroupToFile()
	{
		List<ColumnChunk> ret = new ArrayList<>();
		for (Map.Entry<ColumnDescriptor, InMemPageWriter> entry : this.pageWriters.entrySet())
		{
			ColumnChunk columnChunk = new ColumnChunk();
			InMemPageWriter writer = entry.getValue();

			for (DataPage dataPage : writer.getPages())
			{
				PageHeader pageHeader = new PageHeader();
				pageHeader.setType(PageType.DATA_PAGE);
				pageHeader.setUncompressed_page_size(dataPage.getUncompressedSize());
				pageHeader.setUncompressed_page_size(dataPage.getCompressedSize());
			}
		}

		return Collections.unmodifiableList(ret);
	}
}
