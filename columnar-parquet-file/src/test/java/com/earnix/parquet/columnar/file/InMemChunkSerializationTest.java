package com.earnix.parquet.columnar.file;

import com.earnix.parquet.columnar.file.writer.ParquetFileColumnarWriterFactory;
import com.earnix.parquet.columnar.file.reader.IndexedParquetColumnarFileReader;
import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkValuesReaderImpl;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

public class InMemChunkSerializationTest
{
	@Test
	public void testInMemChunkSerialization() throws IOException
	{
		Path tmp = Files.createTempFile("parquetFile", ".parquet");
		try
		{
			String colName = "chicken";
			List<Type> col = Collections.singletonList(
					new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, colName));
			MessageType messageType = new MessageType("root", col);
			ColumnDescriptor columnDescriptor = messageType.getColumns().get(0);
			try (ParquetColumnarWriter writer = ParquetFileColumnarWriterFactory.createWriter(tmp, messageType,
					CompressionCodec.ZSTD, true))
			{
				writer.writeRowGroup(1, rowGroupWriter -> rowGroupWriter.writeValues(
						chunkWriter -> chunkWriter.writeColumn(columnDescriptor, new double[] { 1 })));

				double[] randomNums = new double[] { 1, -100, 4, 9394, 3412, 323265 };
				Random rand = new Random();
				double[] randChunkData = IntStream.range(0, 10_000)
						.mapToDouble(i -> randomNums[rand.nextInt(randomNums.length)]).toArray();

				writer.writeRowGroup(randChunkData.length, rowGroupWriter -> rowGroupWriter.writeValues(
						chunkWriter -> chunkWriter.writeColumn(columnDescriptor, randChunkData)));
				writer.finishAndWriteFooterMetadata();
			}
			IndexedParquetColumnarFileReader reader = new IndexedParquetColumnarFileReader(tmp);
			int rowGroup = 0;
			assertEquals(reader, rowGroup);
			rowGroup = 1;
			assertEquals(reader, rowGroup);
		}
		finally
		{
			Files.deleteIfExists(tmp);
		}
	}

	private static void assertEquals(IndexedParquetColumnarFileReader reader, int rowGroup) throws IOException
	{
		InMemChunk inMemChunk = reader.readInMem(rowGroup, reader.getDescriptor(0));
		InMemChunk copied = SerializationUtils.clone(inMemChunk);
		Assert.assertEquals(inMemChunk.getDescriptor(), copied.getDescriptor());
		assertEquals(inMemChunk, copied);
	}

	private static void assertEquals(InMemChunk inMemChunk, InMemChunk copied)
	{
		ChunkValuesReader orig = new ChunkValuesReaderImpl(inMemChunk);
		ChunkValuesReader copy = new ChunkValuesReaderImpl(copied);

		boolean next = false;
		do
		{
			Assert.assertEquals(orig.getDouble(), copy.getDouble(), 0d);
			boolean origNext = orig.next();
			boolean copyNext = copy.next();
			Assert.assertEquals(origNext, copyNext);
			next = origNext;
		}
		while (next);
	}
}
