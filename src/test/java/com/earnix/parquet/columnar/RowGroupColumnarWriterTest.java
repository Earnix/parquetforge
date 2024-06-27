package com.earnix.parquet.columnar;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

public class RowGroupColumnarWriterTest
{
	@Test
	public void chicken()
	{
		MessageType messageType = new MessageType("root",
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, "testDouble"));
		ParquetProperties properties = ParquetProperties.builder().build();
		RowGroupColumnarWriter writer = new RowGroupColumnarWriterImpl(messageType, properties, 3);
		writer.writeColumn("testDouble", new double[] { 1.0, 2.0, 3.0 });
		writer.finishGroup();
	}
}
