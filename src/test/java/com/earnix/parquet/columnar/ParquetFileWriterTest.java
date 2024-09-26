package com.earnix.parquet.columnar;

import com.earnix.parquet.columnar.reader.ParquetColumarFileReader;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.reader.processors.ParquetFileProcessors;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import com.earnix.parquet.columnar.writer.ParquetFileColumnarWriterImpl;
import com.earnix.parquet.columnar.writer.columnchunk.NullableIterators;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupWriter;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

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
			reader.processFile((ParquetFileProcessors.ProcessPerChunk) chunk -> System.out
					.println(chunk.getDescriptor() + " TotalValues:" + chunk.getTotalValues()));
		}
		finally
		{
			Files.deleteIfExists(out);
		}
	}

	private static void createTestFile(Path out) throws IOException
	{
		List<PrimitiveType> cols = Arrays.asList(
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, "Chicken"),
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "taco"),
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "potato"),
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, "foobar"),
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "testStr"));
		try (ParquetColumnarWriter writer = new ParquetFileColumnarWriterImpl(out, cols);)
		{
			RowGroupWriter groupWriter;
			groupWriter = writer.startNewRowGroup(2);
			groupWriter.writeColumn(
					columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(0).getName(), new double[] { 1, 2.0 }));
			groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(1).getName(),
					Arrays.asList(false, null).iterator()));
			groupWriter.writeColumn(
					columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(2).getName(), new int[] { 4, 6 }));

			NullableIterators.NullableLongIterator nullableLongIterator = new NullableIterators.NullableLongIterator()
			{
				int element = 0;

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
			};
			groupWriter.writeColumn(
					columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(3).getName(), nullableLongIterator));
			groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(4).getName(),
					new String[] { "burrito", "taco" }));
			writer.finishRowGroup();

			groupWriter = writer.startNewRowGroup(1);
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
			writer.finishRowGroup();

			int lotsOfRows = 10_000;
			groupWriter = writer.startNewRowGroup(lotsOfRows);
			groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(0).getName(),
					IntStream.range(0, lotsOfRows).mapToDouble(Double::valueOf).toArray()));
			groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(1).getName(),
					IntStream.range(0, lotsOfRows).mapToObj(i -> i % 2 == 0).iterator()));
			groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(2).getName(),
					IntStream.range(0, lotsOfRows).toArray()));
			groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(3).getName(),
					LongStream.range(0, lotsOfRows).toArray()));
			groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeStringColumn(cols.get(4).getName(),
					IntStream.range(0, lotsOfRows).mapToObj(i -> "Cheeseburger" + i % 10).iterator()));
			writer.finishRowGroup();
			writer.finishAndWriteFooterMetadata();
		}
	}
}
