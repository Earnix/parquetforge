package com.earnix.parquet.columnar;

import com.earnix.parquet.columnar.reader.ParquetColumarFileReader;
import com.earnix.parquet.columnar.reader.chunk.internal.HackyParquetExtendedColumnReader;
import com.earnix.parquet.columnar.reader.processors.ParquetColumnarProcessors;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import com.earnix.parquet.columnar.writer.ParquetFileColumnarWriterImpl;
import com.earnix.parquet.columnar.writer.columnchunk.NullableIterators;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupWriter;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.junit.Assert.assertEquals;

public class ParquetFileWriterTest
{
	@Test
	public void sanityCreateCheck() throws IOException
	{
		Path out = Files.createTempFile("testParquetFile", ".parquet");
		try
		{
			createTestFile(out);

			ParquetColumarFileReader reader = new ParquetColumarFileReader(out);
			reader.processFile((ParquetColumnarProcessors.ProcessPerChunk) chunk -> {
				System.out.println(chunk.getDescriptor() + " TotalValues:" + chunk.getTotalValues());
				if (chunk.getDescriptor().getPrimitiveType()
						.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.DOUBLE)
				{
					ColumnReaderImpl colReader = new HackyParquetExtendedColumnReader(chunk);
					for (int i = 0; i < chunk.getTotalValues(); i++)
					{
						colReader.consume();
						System.out.println(chunk.getDescriptor() + " Value: " + colReader.getDouble());
					}
				}
			});
		}
		finally
		{
			Files.deleteIfExists(out);
		}
	}

	@Test
	public void testGivenParquetFile_whenProcessingByRowGroup_thenAllRowGroupsAreProcessed() throws IOException
	{
		Path out = Files.createTempFile("testParquetFile", ".parquet");
		try
		{
			List<RowGroupForTesting> inputRowGroups = createTestFile(out);
			List<RowGroupForTesting> outputRowGroups = new ArrayList<>();
			ParquetColumarFileReader reader = new ParquetColumarFileReader(out);
			reader.processFile((ParquetColumnarProcessors.ProcessPerRowGroup) rowGroup -> {
				RowGroupForTesting rowGroupForTesting = new RowGroupForTesting(rowGroup.getNumRows());

				rowGroup.forEachColumnChunk((columnDescriptor, chunk) -> {
				ColumnReaderImpl colReader = new HackyParquetExtendedColumnReader(chunk);
				List<Object> values = new ArrayList<>();
				for (int i = 0; i < chunk.getTotalValues(); i++)
				{
					System.out.println("Read 1 " + columnDescriptor.getPrimitiveType().getPrimitiveTypeName());
					System.out.println("Read 11 " + columnDescriptor.getPrimitiveType().getPrimitiveTypeName());
					if (columnDescriptor.getPrimitiveType().getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.INT64))
					{
						colReader.consume();
						//System.out.println("Read 2 = " + colReader.getLong());
					}
//					if (columnDescriptor.getPrimitiveType().getPrimitiveTypeName().equals(PrimitiveType.PrimitiveTypeName.BOOLEAN))
//					{
//						colReader.consume();
//						//System.out.println("Read 3  = " + colReader.getBoolean());
//					}
					PrimitiveType.PrimitiveTypeName primitiveTypeName = columnDescriptor.getPrimitiveType().getPrimitiveTypeName();
					values.add(GeneralColumnReader.getValue(colReader, primitiveTypeName,columnDescriptor.getMaxDefinitionLevel(), GeneralColumnReader.convertToObjectClass(primitiveTypeName)));
				}
				rowGroupForTesting.addChunk(new ColumnChunkForTesting(
						columnDescriptor.getPrimitiveType().getName(),
						values));
				});
				outputRowGroups.add(rowGroupForTesting);
			});

			assertEquals(inputRowGroups, outputRowGroups);
		}
		finally
		{
			Files.deleteIfExists(out);
		}
	}


	private static List<RowGroupForTesting> createTestFile(Path out) throws IOException
	{
		List<PrimitiveType> cols = Arrays.asList(
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, "DOUBLE_1"),
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "BOOLEAN_2"),
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "INT32_3"),
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "INT64_4"),
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "BINARY_5"));
		try (ParquetColumnarWriter writer = new ParquetFileColumnarWriterImpl(out, cols))
		{
			List<RowGroupForTesting> rowGroups = new ArrayList<>();
			rowGroups.add(writeRowGroup1(cols, writer));
//			writeRowGroup2(cols, writer);
//			writeRowGroup3(cols, writer);

			writer.finishAndWriteFooterMetadata();
			return rowGroups;
		}
	}

	private static RowGroupForTesting writeRowGroup1(List<PrimitiveType> cols, ParquetColumnarWriter parquetColumnarWriter) throws IOException
	{

		int NUM_ROWS_2 = 2;
		RowGroupForTesting rowGroup = new RowGroupForTesting(NUM_ROWS_2);

		RowGroupWriter groupWriter = parquetColumnarWriter.startNewRowGroup(NUM_ROWS_2);
		groupWriter.writeColumn(
				columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(0).getName(), new double[] { 1, 1 }));
		rowGroup.addChunk(new ColumnChunkForTesting(cols.get(0).getName(), Arrays.stream(new double[] { 1, 1 }).boxed().collect(Collectors.toList())));

		groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(1).getName(),
				Arrays.asList(false, null).iterator()));
		rowGroup.addChunk(new ColumnChunkForTesting(cols.get(1).getName(), Arrays.asList(false, null)));

		groupWriter.writeColumn(
				columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(2).getName(), new int[] { 4, 6 }));
		rowGroup.addChunk(new ColumnChunkForTesting(cols.get(2).getName(), Arrays.stream(new int[] { 4, 6 }).boxed().collect(Collectors.toList())));

		NullableLongIteratorImpl nullableLongIterator1 = new NullableLongIteratorImpl();
		NullableLongIteratorImpl nullableLongIterator2 = new NullableLongIteratorImpl();
		groupWriter.writeColumn(
				columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(3).getName(), nullableLongIterator1));
		List<Long> longVals = new ArrayList<>();
		while (nullableLongIterator2.next())
		{
			if (nullableLongIterator2.isNull())
			{
				longVals.add(null);
			}
			else
			{
				longVals.add(nullableLongIterator2.getValue());
			}
		}
		rowGroup.addChunk(new ColumnChunkForTesting(cols.get(3).getName(), longVals));

		groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(4).getName(),
				new String[] { "burrito", "taco" }));
		rowGroup.addChunk(new ColumnChunkForTesting(cols.get(4).getName(), Arrays.asList(new String[] { "burrito", "taco" })));

		parquetColumnarWriter.finishRowGroup();
		return rowGroup;
	}

	private static class NullableLongIteratorImpl implements NullableIterators.NullableLongIterator
	{
		private int element = 0;

		@Override
		public long getValue()
		{

			return 1;
		}

		@Override
		public boolean isNull()
		{
			return element % 2 == 0;
		}

		@Override
		public boolean next()
		{
			return element++ < 2;
		}
	}


	private static void writeRowGroup2(List<PrimitiveType> cols, ParquetColumnarWriter parquetColumnarWriter) throws IOException
	{
		RowGroupWriter groupWriter = parquetColumnarWriter.startNewRowGroup(1);
		groupWriter.writeColumn(
				columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(0).getName(), new double[] { 30, }));
		groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(1).getName(),
				Arrays.asList(Boolean.FALSE).iterator()));
		groupWriter.writeColumn(
				columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(2).getName(), new int[] { 4, }));
		groupWriter.writeColumn(
				columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(3).getName(), new long[] { 4, }));
		groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(4).getName(),
				new String[] { "cheezburger", }));
		parquetColumnarWriter.finishRowGroup();
	}

	private static void writeRowGroup3(List<PrimitiveType> cols, ParquetColumnarWriter writer) throws IOException
	{
		int LOTS_OF_ROWS = 10_000;
		RowGroupWriter groupWriter = writer.startNewRowGroup(LOTS_OF_ROWS);
		groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(0).getName(),
				IntStream.range(0, LOTS_OF_ROWS).mapToDouble(Double::valueOf).toArray()));
		groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(1).getName(),
				IntStream.range(0, LOTS_OF_ROWS).mapToObj(i -> i % 2 == 0).iterator()));
		groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(2).getName(),
				IntStream.range(0, LOTS_OF_ROWS).toArray()));
		groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(3).getName(),
				LongStream.range(0, LOTS_OF_ROWS).toArray()));
		groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeStringColumn(cols.get(4).getName(),
				IntStream.range(0, LOTS_OF_ROWS).mapToObj(i -> "Cheeseburger" + i % 10).iterator()));
		writer.finishRowGroup();
	}

}
