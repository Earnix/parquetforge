package com.earnix.parquet.columnar;

import com.earnix.parquet.columnar.reader.ParquetColumarFileReader;
import com.earnix.parquet.columnar.reader.chunk.InMemRowGroup;
import com.earnix.parquet.columnar.reader.chunk.internal.HackyParquetExtendedColumnReader;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.reader.processors.ParquetColumnarProcessors;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import com.earnix.parquet.columnar.writer.ParquetFileColumnarWriterImpl;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupWriter;
import org.apache.parquet.column.ColumnDescriptor;
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
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.earnix.parquet.columnar.GeneralColumnReader.getValue;
import static com.earnix.parquet.columnar.utils.ColumnWritingUtil.writeBinaryColumn;
import static com.earnix.parquet.columnar.utils.ColumnWritingUtil.writeBooleanColumn;
import static com.earnix.parquet.columnar.utils.ColumnWritingUtil.writeDoubleColumn;
import static com.earnix.parquet.columnar.utils.ColumnWritingUtil.writeInt32Column;
import static com.earnix.parquet.columnar.utils.ColumnWritingUtil.writeInt64Column;
import static com.earnix.parquet.columnar.utils.Utils.processForFile;
import static com.earnix.parquet.columnar.utils.Utils.convertExceptionToRuntime;
import static org.junit.Assert.assertEquals;

public class ParquetFileWriterTest
{
	@Test
	public void sanityCreateCheck() throws IOException
	{
		Path parquetFile = Files.createTempFile("testParquetFile", ".parquet");
		try
		{
			fillWithRowGroups(parquetFile);

			ParquetColumarFileReader reader = new ParquetColumarFileReader(parquetFile);
			reader.processFile((ParquetColumnarProcessors.ChunkProcessor) chunk -> {
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
			Files.deleteIfExists(parquetFile);
		}
	}

	@Test
	public void testGivenParquetFile_whenReadingAndProcessingByRowGroup_thenAllRowGroupsAreProcessed()
	{
		processForFile("testParquetFile", ".parquet", file -> {
			List<RowGroupForTesting> expectedRowGroups = fillWithRowGroups(file);
			List<RowGroupForTesting> actualRowGroups = readingAndProcessByRowGroup(file);
			assertEquals(expectedRowGroups, actualRowGroups);
		});
	}

	private static List<RowGroupForTesting> fillWithRowGroups(Path parquetFile)
	{
		try (ParquetColumnarWriter writer = new ParquetFileColumnarWriterImpl(parquetFile, PARQUET_COLUMNS))
		{
			List<RowGroupForTesting> rowGroups = new ArrayList<>();
			rowGroups.add(writeRowGroup1(writer));
			//			writeRowGroup2(cols, writer);
			//			writeRowGroup3(cols, writer);

			writer.finishAndWriteFooterMetadata();
			return rowGroups;
		}
		catch (Exception ex)
		{
			throw new RuntimeException(ex);
		}
	}

	private static List<RowGroupForTesting> readingAndProcessByRowGroup(Path parquetFile)
	{
		return convertExceptionToRuntime(() -> {
			ParquetColumarFileReader reader = new ParquetColumarFileReader(parquetFile);

			List<RowGroupForTesting> actualRowGroups = new ArrayList<>();
			ParquetColumnarProcessors.RowGroupProcessor byRowGroupProcessor = rowGroup -> actualRowGroups.add(processRowGroup(rowGroup));
			reader.processFile(byRowGroupProcessor);
			return actualRowGroups;
		});
	}

	private static RowGroupForTesting processRowGroup(InMemRowGroup rowGroup)
	{
		RowGroupForTesting rowGroupForTesting = new RowGroupForTesting(rowGroup.getNumRows());
		BiConsumer<ColumnDescriptor, InMemChunk> chunkProcessor = (columnDescriptor, chunk) -> rowGroupForTesting.addChunk(processChunk(columnDescriptor, chunk));
		rowGroup.forEachColumnChunk(chunkProcessor);
		return rowGroupForTesting;
	}

	private static ColumnChunkForTesting processChunk(ColumnDescriptor columnDescriptor, InMemChunk chunk)
	{
		ColumnReaderImpl colReader = new HackyParquetExtendedColumnReader(chunk);

		return new ColumnChunkForTesting(
				columnDescriptor.getPrimitiveType().getName(),
				getChunkValues(columnDescriptor, chunk, colReader));
	}

	private static List<Object> getChunkValues(ColumnDescriptor columnDescriptor, InMemChunk chunk, ColumnReaderImpl colReader)
	{
		return LongStream.range(0, chunk.getTotalValues())
				.mapToObj(index -> getValue(colReader, columnDescriptor))
				.collect(Collectors.toList());
	}

	private static final String COL_1_DOUBLE = "COL_1_DOUBLE";
	private static final String COL_BOOLEAN_2 = "COL_2_BOOLEAN";
	private static final String COL_3_INT_32 = "COL_3_INT32";
	private static final String COL_4_INT_64 = "COL_4_INT64";
	private static final String COL_5_BINARY = "COL_5_BINARY";
	private static final List<PrimitiveType> PARQUET_COLUMNS = Arrays.asList(
			new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, COL_1_DOUBLE),
			new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, COL_BOOLEAN_2),
			new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, COL_3_INT_32),
			new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, COL_4_INT_64),
			new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, COL_5_BINARY)
	);

	private static RowGroupForTesting writeRowGroup1(ParquetColumnarWriter parquetColumnarWriter) throws IOException
	{
		List<Function<RowGroupWriter, ColumnChunkForTesting>> chunkBuilders = Arrays.asList(
				writer -> writeDoubleColumn(writer, COL_1_DOUBLE, new double[]{ 1, 1 }),
				writer -> writeBooleanColumn(writer, COL_BOOLEAN_2, Arrays.asList(false, null)),
				writer -> writeInt32Column(writer, COL_3_INT_32, new int[]{ 4, 6  }),
				writer -> writeInt64Column(writer, COL_4_INT_64, new NullableLongIteratorImpl()),
				writer -> writeBinaryColumn(writer, COL_5_BINARY, new String[] { "burrito", "taco"})
		);

		return writeRowGroup(2, chunkBuilders, parquetColumnarWriter);
	}

	private static RowGroupForTesting writeRowGroup(int rowsNumber, List<Function<RowGroupWriter, ColumnChunkForTesting>> chunkBuilders, ParquetColumnarWriter parquetColumnarWriter) throws IOException
	{
		RowGroupForTesting expectedRowGroup = new RowGroupForTesting(rowsNumber);
		RowGroupWriter groupWriter = parquetColumnarWriter.startNewRowGroup(rowsNumber);
		chunkBuilders.forEach(builder -> expectedRowGroup.addChunk(builder.apply(groupWriter)));
		parquetColumnarWriter.finishRowGroup();
		return expectedRowGroup;
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
