package com.earnix.parquet.columnar.columnchunk;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.DoubleSupplier;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnWriteStoreV2;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import com.earnix.parquet.columnar.compressors.CompressorZstdImpl;
import com.earnix.parquet.columnar.page.InMemPageWriter;

public class ColumnChunkWriterImpl implements ColumnChunkWriter
{
	private final MessageType messageType;
	private final ParquetProperties parquetProperties;

	/**
	 * Number of rows in this row group
	 */
	private final long numRows;
	private final AtomicLong totalBytes = new AtomicLong();

	public ColumnChunkWriterImpl(MessageType messageType, ParquetProperties parquetProperties, long numRows)
	{
		this.messageType = messageType;
		this.parquetProperties = parquetProperties;
		this.numRows = numRows;
		CompressionCodecFactory.BytesInputCompressor compressor = new CompressorZstdImpl();
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, double[] vals)
	{
		if (vals.length != numRows)
			throw new IllegalArgumentException();
		return trackBytesWritten(writeColumn(columnName, new DoubleSupplier()
		{
			int idx = 0;

			@Override
			public double getAsDouble()
			{
				return vals[idx++];
			}
		}));
	}

	private ColumnChunkPages trackBytesWritten(ColumnChunkPages pages)
	{
		this.totalBytes.addAndGet(pages.compressedBytes());
		return pages;
	}

	public ColumnChunkPages writeColumn(String columnName, DoubleSupplier doubleSupplier)
	{
		try (InMemPageWriter writer = new InMemPageWriter())
		{
			PrimitiveType type = (PrimitiveType) messageType.getType(columnName);
			ColumnDescriptor path = new ColumnDescriptor(new String[] { columnName }, type, 0, 0);

			PageWriteStore pageWriteStore = descriptor -> {
				if (!path.equals(descriptor))
				{
					throw new IllegalArgumentException("unexpected descriptor: " + descriptor + ". expected: " + path);
				}
				return writer;
			};

			MessageType dummyMessageType = new MessageType(messageType.getName(), type);
			// A hacky way to use the page limit/flush logic without changing the column writer impl
			try (ColumnWriteStore writeStore = new ColumnWriteStoreV2(dummyMessageType, pageWriteStore, parquetProperties);
					ColumnWriter columnWriter = writeStore.getColumnWriter(path);)
			{
				for (long i = 0; i < numRows; i++)
				{
					columnWriter.write(doubleSupplier.getAsDouble(), 0, 0);
					writeStore.endRecord();
				}
				writeStore.flush();
			}
			return new ColumnChunkPages(path, writer.getDictionaryPage(), writer.getPages());
		}
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, int[] vals)
	{
		return null;
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, long[] vals)
	{
		return null;
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, String[] vals)
	{
		return null;
	}

	@Override
	public long totalBytesInRowGroup()
	{
		return totalBytes.get();
	}
}
