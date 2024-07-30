package com.earnix.parquet.columnar;

import com.earnix.parquet.columnar.reader.ColumnChunkReader;
import com.earnix.parquet.columnar.reader.ColumnChunkReaderFactory;
import com.earnix.parquet.columnar.reader.ReadableDataPage;
import com.earnix.parquet.columnar.reader.UncompressedColumn;
import com.earnix.parquet.columnar.reader.UncompressedColumnFactory;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkPages;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriter;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriterImpl;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

public class ColumnChunkWriterReaderTest
{
	@Test
	public void testSimpleWriteAndRead() throws Exception
	{
		double[] vals = { 1.0, 2.0, 3.0 };
		validateWriteRead(vals);
	}

	private static void validateWriteRead(double[] vals) throws Exception
	{
		CompressionCodec codec = CompressionCodec.UNCOMPRESSED;
		MessageType messageType = new MessageType("root",
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, "testDouble"));
		ParquetProperties properties = ParquetProperties.builder().build();
		ColumnChunkWriter writer = new ColumnChunkWriterImpl(messageType, codec, properties, 3);
		ColumnChunkPages pages = writer.writeColumn("testDouble", vals);
		System.out.println("Bytes to write: " + pages.totalBytesForStorage());

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream((int) pages.totalBytesForStorage());
		pages.writeToOutputStream(byteArrayOutputStream);
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());

		UncompressedColumnFactory columnFactory = new UncompressedColumnFactory(pages.getColumnDescriptor());
		UncompressedColumn col = columnFactory.build(byteArrayInputStream, pages.totalBytesForStorage(), codec);

		ColumnChunkReader reader = col.getDataPageHeaderList().get(0).buildReader();
		for (double val : vals)
		{
			Assert.assertTrue(reader.next());
			Assert.assertFalse(reader.isNull());
			Assert.assertEquals(reader.getDouble(), val, 0d);
		}

		Assert.assertFalse(reader.next());
	}
}
