package com.earnix.parquet.columnar.columnchunk;

import java.util.Arrays;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnWriteStoreV2;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

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
	private final CompressionCodec compressionCodec;

	public ColumnChunkWriterImpl(MessageType messageType, CompressionCodec compressionCodec,
			ParquetProperties parquetProperties, long numRows)
	{
		this.messageType = messageType;
		this.compressionCodec = compressionCodec;
		this.parquetProperties = parquetProperties;
		this.numRows = numRows;
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, double[] vals)
	{
		if (vals.length != numRows)
			throw new IllegalArgumentException();
		return trackBytesWritten(writeColumn(columnName, DoubleStream.of(vals).iterator()));
	}

	private ColumnChunkPages trackBytesWritten(ColumnChunkPages pages)
	{
		this.totalBytes.addAndGet(pages.totalBytesForStorage());
		return pages;
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, PrimitiveIterator.OfDouble doubleIterator)
	{
		return internalWriteColumn(columnName, doubleIterator,
				(colwriter, it) -> colwriter.write(it.nextDouble(), 0, 0));
	}

	private <T, I extends Iterator<T>> ColumnChunkPages internalWriteColumn(String columnName, I primitiveIterator,
			BiConsumer<ColumnWriter, I> recordCallback)
	{
		try (InMemPageWriter writer = new InMemPageWriter(compressionCodec))
		{
			PrimitiveType type = (PrimitiveType) messageType.getType(columnName);
			ColumnDescriptor path = new ColumnDescriptor(new String[] { columnName }, type, 1, 0);

			PageWriteStore pageWriteStore = descriptor -> {
				if (!path.equals(descriptor))
				{
					throw new IllegalArgumentException("unexpected descriptor: " + descriptor + ". expected: " + path);
				}
				return writer;
			};

			// A hacky way to use the page limit/flush logic without changing the column writer impl
			MessageType dummyMessageType = new MessageType(messageType.getName(), type);
			try (ColumnWriteStore writeStore = new ColumnWriteStoreV2(dummyMessageType, pageWriteStore,
					parquetProperties); //
					ColumnWriter columnWriter = writeStore.getColumnWriter(path);)
			{
				for (long i = 0; i < numRows; i++)
				{
					if (!primitiveIterator.hasNext())
						throw new IllegalArgumentException("too few values for " + columnName);
					recordCallback.accept(columnWriter, primitiveIterator);
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
		return writeColumn(columnName, IntStream.of(vals).iterator());
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, PrimitiveIterator.OfInt iterator)
	{
		return internalWriteColumn(columnName, iterator, (colwriter, it) -> colwriter.write(it.nextInt(), 0, 0));
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, long[] vals)
	{
		return writeColumn(columnName, LongStream.of(vals).iterator());
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, PrimitiveIterator.OfLong iterator)
	{
		return internalWriteColumn(columnName, iterator, (colwriter, it) -> colwriter.write(it.nextLong(), 0, 0));
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, String[] vals)
	{
		return writeColumn(columnName, Arrays.asList(vals).iterator());
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, Iterator<String> vals)
	{
		return internalWriteColumn(columnName, vals,
				(columnWriter, stringIterator) -> columnWriter.write(Binary.fromString(stringIterator.next()), 0, 0));
	}

	@Override
	public long totalBytesInRowGroup()
	{
		return totalBytes.get();
	}
}
