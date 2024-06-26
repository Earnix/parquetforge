package com.earnix.parquet.columnar;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.DoubleSupplier;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnWriteStoreV1;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.schema.MessageType;

import com.earnix.parquet.columnar.compressors.CompressorZstdImpl;
import com.earnix.parquet.columnar.page.InMemPageWriter;
import org.apache.parquet.schema.PrimitiveType;

public class RowGroupColumnarWriterImpl implements RowGroupColumnarWriter
{
	private final MessageType messageType;
	private final ConcurrentMap<ColumnDescriptor, InMemPageWriter> pageWriters = new ConcurrentHashMap<>();
	private final ColumnWriteStore writeStore;
	private final int numRows;

	public RowGroupColumnarWriterImpl(MessageType messageType, ParquetProperties parquetProperties, int numRows)
	{
		this.messageType = messageType;
		this.numRows = numRows;
		CompressionCodecFactory.BytesInputCompressor compressor = new CompressorZstdImpl();

		PageWriteStore pageWriteStore = path -> pageWriters.computeIfAbsent(path, k -> new InMemPageWriter());
		writeStore = new ColumnWriteStoreV1(messageType, pageWriteStore, parquetProperties);
	}

	@Override
	public void writeColumn(String columnName, double[] vals)
	{

	}

	public void writeColumn(String columnName, DoubleSupplier doubleSupplier)
	{
		ColumnWriter columnWriter = writeStore.getColumnWriter(new ColumnDescriptor(new String[] { columnName },
				(PrimitiveType) messageType.getType(columnName), 1, 0));
		for (int i = 0; i < numRows; i++)
		{
			columnWriter.write(doubleSupplier.getAsDouble(), 1, 0);
		}
		columnWriter.close();
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
}
