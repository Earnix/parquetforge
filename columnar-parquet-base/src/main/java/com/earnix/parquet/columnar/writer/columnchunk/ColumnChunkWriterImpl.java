package com.earnix.parquet.columnar.writer.columnchunk;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

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

public class ColumnChunkWriterImpl implements com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriter
{
	private final MessageType messageType;
	private final ParquetProperties parquetProperties;

	/**
	 * Number of rows in this row group
	 */
	private final long numRows;
	private final CompressionCodec compressionCodec;

	/**
	 * Create a new chunk writer impl. This is an abstraction for writing a row group
	 * 
	 * @param messageType the schema of the message (note that only one level is supported - no structured data.
	 *            optional is supported)
	 * @param compressionCodec the compression codec to compress the data with
	 * @param parquetProperties the properties for parquet encoding
	 * @param numRows the number of rows that are in this row group
	 */
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
		validateArrLen(vals);
		return writeColumn(columnName, DoubleStream.of(vals).iterator());
	}

	private void validateArrLen(Object vals)
	{
		if (Array.getLength(vals) != numRows)
			throw new IllegalArgumentException();
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, PrimitiveIterator.OfDouble doubleIterator)
	{
		return writeColumn(columnName, NullableIterators.wrapDoubleIterator(doubleIterator));
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, NullableIterators.NullableDoubleIterator iterator)
	{
		return internalWriteColumn(columnName, iterator,
				(colwriter, it, defLevel) -> colwriter.write(it.getValue(), 0, defLevel));
	}

	private <I extends NullableIterators.NullableIterator> ColumnChunkPages internalWriteColumn(String columnName,
			I primitiveIterator, RecordConsumer<I> recordCallback)
	{
		try (InMemPageWriter writer = new InMemPageWriter(compressionCodec))
		{
			ColumnDescriptor path = messageType.getColumnDescription(new String[] { columnName });

			PageWriteStore pageWriteStore = descriptor -> {
				if (!path.equals(descriptor))
				{
					throw new IllegalArgumentException("unexpected descriptor: " + descriptor + ". expected: " + path);
				}
				return writer;
			};

			// A hacky way to use the page limit/flush logic without changing the column writer impl
			MessageType dummyMessageType = new MessageType(messageType.getName(), path.getPrimitiveType());
			try (ColumnWriteStore writeStore = new ColumnWriteStoreV2(dummyMessageType, pageWriteStore,
					parquetProperties); //
					ColumnWriter columnWriter = writeStore.getColumnWriter(path))
			{
				for (long i = 0; i < numRows; i++)
				{
					if (!primitiveIterator.next())
						throw new IllegalArgumentException("too few values for " + columnName);
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
				}
				writeStore.flush();
			}
			return new ColumnChunkPages(path, writer.getDictionaryPage(), writer.getPages());
		}
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, int[] vals)
	{
		validateArrLen(vals);
		return writeColumn(columnName, IntStream.of(vals).iterator());
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, PrimitiveIterator.OfInt iterator)
	{
		return writeColumn(columnName, NullableIterators.wrapIntegerIterator(iterator));
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, NullableIterators.NullableIntegerIterator iterator)
	{
		return internalWriteColumn(columnName, iterator,
				(colWriter, it, defLevel) -> colWriter.write(it.getValue(), 0, defLevel));
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, long[] vals)
	{
		validateArrLen(vals);
		return writeColumn(columnName, LongStream.of(vals).iterator());
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, PrimitiveIterator.OfLong iterator)
	{
		return writeColumn(columnName, NullableIterators.wrapLongIterator(iterator));
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, NullableIterators.NullableLongIterator iterator)
	{
		return internalWriteColumn(columnName, iterator,
				(colWriter, it, defLevel) -> colWriter.write(it.getValue(), 0, defLevel));
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, String[] vals)
	{

		validateArrLen(vals);
		return writeStringColumn(columnName, Arrays.asList(vals).iterator());
	}

	@Override
	public ColumnChunkPages writeStringColumn(String columnName, Iterator<String> vals)
	{
		return internalWriteColumn(columnName, NullableIterators.wrapStringIterator(vals),
				(columnWriter, stringIterator, defLevel) -> columnWriter.write(Binary.fromString(stringIterator.getValue()), 0, defLevel));
	}

	@Override
	public ColumnChunkPages writeColumn(String columnName, boolean[] vals)
	{
		return writeColumn(columnName, new Iterator<Boolean>()
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

	public ColumnChunkPages writeColumn(String columnName, Iterator<Boolean> iterator)
	{
		return internalWriteColumn(columnName, NullableIterators.wrapBooleanIterator(iterator),
				(columnWriter, boolIterator, defLevel) -> columnWriter.write(boolIterator.getValue(), 0, defLevel));
	}

	interface RecordConsumer<I extends NullableIterators.NullableIterator>
	{
		void recordCallback(ColumnWriter columnWriter, I iterator, int definitionLevel);
	}
}
