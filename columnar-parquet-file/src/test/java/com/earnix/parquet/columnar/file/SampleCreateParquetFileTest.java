package com.earnix.parquet.columnar.file;

import com.earnix.parquet.columnar.file.writer.ParquetFileColumnarWriterFactory;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;

public class SampleCreateParquetFileTest
{
	private Path parquetFile;

	@Before
	public void setUp() throws Exception
	{
		parquetFile = Files.createTempFile("parquetfile", ".parquet");
	}

	@After
	public void tearDown() throws Exception
	{
		Files.deleteIfExists(parquetFile);
	}

	@Test
	public void exampleCreateParquetFileTest() throws Exception
	{
		List<PrimitiveType> cols = asList(
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, "double"),
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "boolean"));
		MessageType messageType = new MessageType("root", Collections.unmodifiableList(cols));
		try (ParquetColumnarWriter rowGroupWriter = ParquetFileColumnarWriterFactory.createWriter(parquetFile, cols,
				false))
		{

			rowGroupWriter.writeRowGroup(1, rgw -> {
				// write columns in order opposite to the schema.
				rgw.writeValues(
						writer -> writer.writeColumn(messageType.getColumnDescription(new String[] { "boolean" }),
								new boolean[] { true }));
				rgw.writeValues(
						writer -> writer.writeColumn(messageType.getColumnDescription(new String[] { "double" }),
								new double[] { 3 }));
			});
			rowGroupWriter.finishAndWriteFooterMetadata();
		}
	}
}
