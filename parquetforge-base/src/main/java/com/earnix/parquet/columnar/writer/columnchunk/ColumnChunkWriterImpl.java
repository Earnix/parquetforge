package com.earnix.parquet.columnar.writer.columnchunk;

import com.earnix.parquet.columnar.writer.page.InMemPageWriter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnWriteStoreV2;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.Arrays;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class ColumnChunkWriterImpl implements com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriter
{
	private final ParquetProperties parquetProperties;

	/**
	 * Number of rows in this row group
	 */
	private final CompressionCodec compressionCodec;

	/**
	 * Create a new chunk writer impl. This is an abstraction for writing columns to data pages
	 *
	 * @param compressionCodec  the compression codec to compress the data with
	 * @param parquetProperties the properties for parquet encoding
	 */
	public ColumnChunkWriterImpl(CompressionCodec compressionCodec, ParquetProperties parquetProperties)
	{
		this.compressionCodec = compressionCodec;
		this.parquetProperties = parquetProperties;
	}

	@Override
	public ColumnChunkPages writeColumn(ColumnDescriptor column, double[] vals)
	{
		return writeColumn(column, DoubleStream.of(vals).iterator());
	}

	@Override
	public ColumnChunkPages writeColumn(ColumnDescriptor column, PrimitiveIterator.OfDouble doubleIterator)
	{
		return writeColumn(column, NullableIterators.wrapDoubleIterator(doubleIterator));
	}

	@Override
	public ColumnChunkPages writeColumn(ColumnDescriptor column, NullableIterators.NullableDoubleIterator iterator)
	{
		return internalWriteColumn(column, iterator, (colwriter, it) -> colwriter.write(it.getValue()));
	}

	private <I extends NullableIterators.NullableIterator> ColumnChunkPages internalWriteColumn(ColumnDescriptor path,
			I primitiveIterator, RecordConsumer<I> recordCallback)
	{
		try (ColumnChunkValuesWriter columnChunkValuesWriter = new ColumnChunkValuesWriter(path, parquetProperties,
				compressionCodec))
		{
			writeRecords(primitiveIterator, recordCallback, columnChunkValuesWriter);
			return columnChunkValuesWriter.finishAndGetPages();
		}
	}

	private <I extends NullableIterators.NullableIterator> void writeRecords(I primitiveIterator,
			RecordConsumer<I> recordCallback, ColumnChunkValuesWriter columnChunkValuesWriter)
	{
		while (primitiveIterator.next())
		{
			if (primitiveIterator.isNull())
			{
				columnChunkValuesWriter.writeNull();
			}
			else
			{
				recordCallback.recordCallback(columnChunkValuesWriter, primitiveIterator);
			}
		}
	}

	@Override
	public ColumnChunkPages writeColumn(ColumnDescriptor column, int[] vals)
	{
		return writeColumn(column, IntStream.of(vals).iterator());
	}

	@Override
	public ColumnChunkPages writeColumn(ColumnDescriptor column, PrimitiveIterator.OfInt iterator)
	{
		return writeColumn(column, NullableIterators.wrapIntegerIterator(iterator));
	}

	@Override
	public ColumnChunkPages writeColumn(ColumnDescriptor column, NullableIterators.NullableIntegerIterator iterator)
	{
		return internalWriteColumn(column, iterator, (colWriter, it) -> colWriter.write(it.getValue()));
	}

	@Override
	public ColumnChunkPages writeColumn(ColumnDescriptor column, long[] vals)
	{
		return writeColumn(column, LongStream.of(vals).iterator());
	}

	@Override
	public ColumnChunkPages writeColumn(ColumnDescriptor column, PrimitiveIterator.OfLong iterator)
	{
		return writeColumn(column, NullableIterators.wrapLongIterator(iterator));
	}

	@Override
	public ColumnChunkPages writeColumn(ColumnDescriptor column, NullableIterators.NullableLongIterator iterator)
	{
		return internalWriteColumn(column, iterator, (colWriter, it) -> colWriter.write(it.getValue()));
	}

	@Override
	public ColumnChunkPages writeColumn(ColumnDescriptor column, String[] vals)
	{
		return writeStringColumn(column, Arrays.asList(vals).iterator());
	}

	@Override
	public ColumnChunkPages writeStringColumn(ColumnDescriptor column, Iterator<String> vals)
	{
		return internalWriteColumn(column, NullableIterators.wrapStringIterator(vals),
				(columnWriter, stringIterator) -> columnWriter.write(Binary.fromString(stringIterator.getValue())));
	}

	@Override
	public ColumnChunkPages writeBinaryColumn(ColumnDescriptor column, Iterator<byte[]> vals)
	{
		return internalWriteColumn(column, NullableIterators.wrapStringIterator(vals),
				(columnWriter, stringIterator) -> columnWriter.write(
						Binary.fromReusedByteArray(stringIterator.getValue())));
	}

	@Override
	public ColumnChunkPages writeColumn(ColumnDescriptor column, boolean[] vals)
	{
		return writeColumn(column, new Iterator<Boolean>()
		{
			private int i = 0;

			@Override
			public boolean hasNext()
			{
				return i < vals.length;
			}

			@Override
			public Boolean next()
			{
				return vals[i++];
			}
		});
	}

	public ColumnChunkPages writeColumn(ColumnDescriptor column, Iterator<Boolean> iterator)
	{
		return internalWriteColumn(column, NullableIterators.wrapBooleanIterator(iterator),
				(columnWriter, boolIterator) -> columnWriter.write(boolIterator.getValue()));
	}

	interface RecordConsumer<I extends NullableIterators.NullableIterator>
	{
		void recordCallback(ColumnChunkValuesWriter columnWriter, I iterator);
	}
}
