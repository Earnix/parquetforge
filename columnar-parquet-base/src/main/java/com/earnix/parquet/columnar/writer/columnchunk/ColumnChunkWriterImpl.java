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
	private static final String DUMMY_COL_NAME = "dummy_col_name";
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
		return internalWriteColumn(column, iterator,
				(colwriter, it, defLevel) -> colwriter.write(it.getValue(), 0, defLevel));
	}

	private <I extends NullableIterators.NullableIterator> ColumnChunkPages internalWriteColumn(ColumnDescriptor path,
			I primitiveIterator, RecordConsumer<I> recordCallback)
	{
		try (InMemPageWriter writer = new InMemPageWriter(compressionCodec))
		{
			writeRecords(primitiveIterator, recordCallback, path, writer);
			return new ColumnChunkPages(path, writer.getDictionaryPage(), writer.getPages());
		}
	}

	private <I extends NullableIterators.NullableIterator> void writeRecords(I primitiveIterator,
			RecordConsumer<I> recordCallback, ColumnDescriptor path, InMemPageWriter writer)
	{
		PageWriteStore pageWriteStore = descriptor -> {
			if (!path.equals(descriptor))
			{
				throw new IllegalArgumentException("unexpected descriptor: " + descriptor + ". expected: " + path);
			}
			return writer;
		};

		// A hacky way to use the page limit/flush logic without changing the column writer impl
		MessageType dummyMessageType = new MessageType(DUMMY_COL_NAME, path.getPrimitiveType());
		try (ColumnWriteStore writeStore = new ColumnWriteStoreV2(dummyMessageType, pageWriteStore, parquetProperties);
				ColumnWriter columnWriter = writeStore.getColumnWriter(path))
		{
			long numVals = 0;
			while (primitiveIterator.next())
			{
				if (primitiveIterator.isNull())
				{
					if (path.getPrimitiveType().getRepetition() == Type.Repetition.REQUIRED)
						throw new IllegalStateException("Field is required!");
					columnWriter.writeNull(0, 0);
				}
				else
				{
					recordCallback.recordCallback(columnWriter, primitiveIterator, path.getMaxDefinitionLevel());
				}
				writeStore.endRecord();
				++numVals;
			}

			if (numVals == 0)
				throw new IllegalArgumentException("A page cannot contain zero values " + path);

			writeStore.flush();
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
		return internalWriteColumn(column, iterator,
				(colWriter, it, defLevel) -> colWriter.write(it.getValue(), 0, defLevel));
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
		return internalWriteColumn(column, iterator,
				(colWriter, it, defLevel) -> colWriter.write(it.getValue(), 0, defLevel));
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
				(columnWriter, stringIterator, defLevel) -> columnWriter.write(
						Binary.fromString(stringIterator.getValue()), 0, defLevel));
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
				(columnWriter, boolIterator, defLevel) -> columnWriter.write(boolIterator.getValue(), 0, defLevel));
	}

	interface RecordConsumer<I extends NullableIterators.NullableIterator>
	{
		void recordCallback(ColumnWriter columnWriter, I iterator, int definitionLevel);
	}
}
