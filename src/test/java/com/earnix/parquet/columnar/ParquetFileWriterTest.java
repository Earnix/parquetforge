package com.earnix.parquet.columnar;

import com.earnix.parquet.columnar.rowgroup.RowGroupWriter;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class ParquetFileWriterTest
{
	@Test
	public void sanityCreateCheck() throws IOException
	{
		// Path out = Paths.get("/Users/andrewp/test2.parquet");
		Path out = Files.createTempFile("testParquetFile", ".parquet");
		try
		{
			createTestFile(out);
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
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, "taco"),
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "potato"),
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "foobar"));
		try (ParquetColumnarWriter writer = new ParquetFileColumnarWriterImpl(out, cols);)
		{
			RowGroupWriter groupWriter;

			groupWriter = writer.startNewRowGroup(2);
			groupWriter.writeColumn(
					columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(0).getName(), new double[] { 1, 2.0 }));
			groupWriter.writeColumn(
					columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(1).getName(), new double[] { 4, 3 }));
			groupWriter.writeColumn(
					columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(2).getName(), new int[] { 4, 6 }));
			groupWriter.writeColumn(
					columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(3).getName(), new long[] { 4, 6 }));

			groupWriter = writer.startNewRowGroup(1);
			groupWriter.writeColumn(
					columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(0).getName(), new double[] { 30, }));
			groupWriter.writeColumn(
					columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(1).getName(), new double[] { 4, }));
			groupWriter.writeColumn(
					columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(2).getName(), new int[] { 4, }));
			groupWriter.writeColumn(
					columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(3).getName(), new long[] { 4, }));

			int lotsOfRows = 10_000;
			groupWriter = writer.startNewRowGroup(lotsOfRows);
			groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(0).getName(),
					IntStream.range(0, 10_000).mapToDouble(Double::valueOf).toArray()));
			groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(1).getName(),
					IntStream.range(0, 10_000).map(i -> i % 10).mapToDouble(Double::valueOf).toArray()));
			groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(2).getName(),
					IntStream.range(0, 10_000).toArray()));
			groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(3).getName(),
					LongStream.range(0, 10_000).toArray()));

			writer.finishAndWriteFooterMetadata();
		}
	}
}
