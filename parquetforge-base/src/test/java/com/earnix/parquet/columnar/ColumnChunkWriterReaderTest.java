package com.earnix.parquet.columnar;

import com.earnix.parquet.columnar.reader.chunk.ChunkReader;
import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkValuesReaderFactory;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkPages;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriter;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriterImpl;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class ColumnChunkWriterReaderTest
{
	@Test
	public void testSimpleWriteAndRead() throws Exception
	{
		double[] vals = { 1.0, 2.0, 3.0 };
		validateWriteRead(vals);
		Assert.assertFalse(getChunkValuesReader(vals).isDictionaryIdSupported());
	}

	@Test
	public void testWriteReadWithDictScenario() throws Exception
	{
		Random r = new Random();
		double[] dict = r.doubles(100).toArray();
		double[] vals = new double[1_000_000];

		for (int i = 0; i < vals.length; i++)
		{
			vals[i] = dict[r.nextInt(100)];
		}

		validateWriteRead(vals);

		ChunkValuesReader reader = getChunkValuesReader(vals);
		int valuesRead = 0;
		do
		{
			Assert.assertTrue(reader.isDictionaryIdSupported());
			Assert.assertTrue(reader.getDictionaryId() < dict.length);
			valuesRead++;
		}
		while (reader.next());
		Assert.assertEquals(vals.length, valuesRead);
	}

	@Test
	public void testReadingWithNextTwice() throws IOException
	{
		double[] vals = { 1.1, 2.2, 3.3, 4.4, 5.5, 6.6 };
		ChunkValuesReader reader = getChunkValuesReader(vals);

		assertEquals(1.1, reader.getDouble(), 0d);
		reader.next();
		reader.next();
		assertEquals(3.3, reader.getDouble(), 0d);
	}

	@Test
	public void testGivenChunkReader_whenSkipFewTimes_thenReadValueOfSkipUntilAfterChunkEndsItThrowNoSuchElement()
			throws IOException
	{
		double[] vals = { 1.1, 2.2, 3.3, 4.4, 5.5, 6.6 };
		ChunkValuesReader reader = getChunkValuesReader(vals);

		reader.skip(1);
		assertEquals(2.2, reader.getDouble(), 0);


		reader.skip(2);
		assertEquals(4.4, reader.getDouble(), 0);
		assertEquals(4.4, reader.getDouble(), 0);

		// skipping zero rows should be a noop
		reader.skip(0);
		assertEquals(4.4, reader.getDouble(), 0);

		reader.skip(1);
		assertEquals(5.5, reader.getDouble(), 0);

		assertThrows(NoSuchElementException.class, () -> reader.skip(3));
		assertThrows(NoSuchElementException.class, () -> reader.skip(1));

	}

	private static void validateWriteRead(double[] vals) throws Exception
	{
		ChunkValuesReader reader = getChunkValuesReader(vals);
		for (int i = 0; i < vals.length; i++)
		{
			Assert.assertFalse(reader.isNull());
			Assert.assertEquals(reader.getDouble(), vals[i], 0d);

			boolean expectNext = i != vals.length - 1; // do not expect next on the last iteration
			Assert.assertEquals(expectNext, reader.next());
		}

		Assert.assertFalse(reader.next());
	}

	private static ChunkValuesReader getChunkValuesReader(double[] vals) throws IOException
	{
		CompressionCodec codec = CompressionCodec.UNCOMPRESSED;

		// make the schema of this dummy column. In this case, we simply have a root message which contains one sub
		// column.
		MessageType messageType = new MessageType("root",
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, "testDouble"));

		// Parquet writing properties - default is fine for this case.
		ParquetProperties properties = ParquetProperties.builder().build();

		ColumnChunkWriter writer = new ColumnChunkWriterImpl(codec, properties);
		ColumnChunkPages pages = writer.writeColumn(messageType.getColumnDescription(new String[] { "testDouble" }),
				vals);
		System.out.println("Bytes to write: " + pages.totalBytesForStorage());

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream((int) pages.totalBytesForStorage());
		pages.writeToOutputStream(byteArrayOutputStream);
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());

		InMemChunk chunk = ChunkReader.readChunk(pages.getColumnDescriptor(), byteArrayInputStream,
				pages.totalBytesForStorage(), codec);

		ChunkValuesReader reader = ChunkValuesReaderFactory.createChunkReader(chunk);
		return reader;
	}
}
