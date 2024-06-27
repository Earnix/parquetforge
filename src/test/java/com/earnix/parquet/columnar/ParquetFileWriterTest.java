package com.earnix.parquet.columnar;

import com.earnix.parquet.columnar.rowgroup.RowGroupWriter;
import org.apache.commons.lang3.IntegerRange;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class ParquetFileWriterTest
{
	@Test
	public void chicken() throws IOException
	{
		Path out = Paths.get("/Users/andrewp/test2.parquet");
		List<PrimitiveType> cols = Arrays.asList(
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, "Chicken"),
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, "taco"));
		try (ParquetColumnarWriter writer = new ParquetFileColumnarWriterImpl(out, cols);)
		{
			RowGroupWriter groupWriter;

			groupWriter = writer.startNewRowGroup(1);
			groupWriter.writeColumn(
					columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(0).getName(), new double[] { 1, }));
			groupWriter.writeColumn(
					columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(1).getName(), new double[] { 4, }));

			groupWriter = writer.startNewRowGroup(1);
			groupWriter.writeColumn(
					columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(0).getName(), new double[] { 30, }));
			groupWriter.writeColumn(
					columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(1).getName(), new double[] { 4, }));

			int lotsOfRows = 10_000;
			groupWriter = writer.startNewRowGroup(lotsOfRows);
			groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(0).getName(),
					IntStream.range(0, 10_000).mapToDouble(Double::valueOf).toArray()));
			groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(1).getName(),
					IntStream.range(0, 10_000).map(i -> i % 10).mapToDouble(Double::valueOf).toArray()));

			writer.finishAndWriteFooterMetadata();
		}
	}
}
