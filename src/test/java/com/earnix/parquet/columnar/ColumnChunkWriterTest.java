package com.earnix.parquet.columnar;

import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkPages;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriter;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriterImpl;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

public class ColumnChunkWriterTest
{
	@Test
	public void chicken()
	{
		MessageType messageType = new MessageType("root",
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, "testDouble"));
		ParquetProperties properties = ParquetProperties.builder().build();
		ColumnChunkWriter writer = new ColumnChunkWriterImpl(messageType, CompressionCodec.UNCOMPRESSED, properties, 3);
		ColumnChunkPages pages = writer.writeColumn("testDouble", new double[] { 1.0, 2.0, 3.0 });
		System.out.println("Bytes to write: " + pages.totalBytesForStorage());
	}
}
