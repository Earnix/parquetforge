package com.earnix.parquet.columnar;

import com.earnix.parquet.columnar.rowgroup.RowGroupWriter;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

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
			RowGroupWriter groupWriter = writer.startNewRowGroup(3);
			groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(0).getName(),
					new double[] { 1, 2, 3 }));
			groupWriter.writeColumn(columnChunkWriter -> columnChunkWriter.writeColumn(cols.get(1).getName(),
					new double[] { 4, 5, 6 }));

			writer.finishAndWriteFooterMetadata();
		}
	}
}
