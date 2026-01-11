package com.earnix.parquet.columnar.file;

import com.earnix.parquet.columnar.file.reader.ParquetFileReaderFactory;
import com.earnix.parquet.columnar.file.writer.ParquetFileColumnarWriterFactory;
import com.earnix.parquet.columnar.reader.IndexedParquetColumnarReader;
import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkValuesReaderFactory;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import com.earnix.parquet.columnar.writer.columnchunk.NullableIterators;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertFalse;

public class ChunkValuesReaderTest
{
	private static final String colName = "chicken";

	@Test
	public void testColumnWithNullValues1() throws Exception
	{
		testNullableValues(new Integer[] { null, null, 3 });
	}

	@Test
	public void testColumnWithNullValues2() throws Exception
	{
		testNullableValues(new Integer[] { null });
	}

	@Test
	public void testColumnWithNullValues3() throws Exception
	{
		testNullableValues(new Integer[] { 1, null });
	}

	@Test
	public void testColumnWithNullValues4() throws Exception
	{
		testNullableValues(new Integer[] { null, 1 });
	}

	@Test
	public void testColumnWithNullValues5() throws Exception
	{
		testNullableValues(new Integer[] { 1, null, null, null });
	}

	private static void testNullableValues(Integer[] vals) throws IOException
	{
		Path tmpFile = Files.createTempFile("testNull", ".parquet");
		try
		{
			MessageType messageType = new MessageType("root",
					new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, colName));
			writeValues(tmpFile, messageType, ParquetProperties.builder().build(), vals);

			IndexedParquetColumnarReader reader = ParquetFileReaderFactory.createIndexedColumnarFileReader(tmpFile);
			Assert.assertEquals(1, reader.getNumRowGroups());
			InMemChunk inMemChunk = reader.readInMem(0, messageType.getColumnDescription(new String[] { colName }));
			ChunkValuesReader chunkValuesReader = ChunkValuesReaderFactory.createChunkReader(inMemChunk);
			for (int i = 0; i < vals.length; i++)
			{
				assertEquals(vals[i], chunkValuesReader);
				Assert.assertEquals(i < vals.length - 1, chunkValuesReader.next());
			}
			chunkValuesReader = ChunkValuesReaderFactory.createChunkReader(inMemChunk, vals.length - 1);
			assertEquals(vals[vals.length - 1], chunkValuesReader);
			assertFalse(chunkValuesReader.next());
		}
		finally
		{
			Files.deleteIfExists(tmpFile);
		}
	}

	static void assertEquals(Integer expected, ChunkValuesReader chunkValuesReader)
	{
		Assert.assertEquals(expected == null, chunkValuesReader.isNull());
		if (expected != null)
			Assert.assertEquals((int) expected, chunkValuesReader.getInteger());
	}

	static void writeValues(Path tmpFile, MessageType messageType, ParquetProperties parquetProperties, Integer[] vals)
			throws IOException
	{
		try (ParquetColumnarWriter parquetColumnarWriter = ParquetFileColumnarWriterFactory.createWriter(tmpFile,
				messageType, parquetProperties, CompressionCodec.ZSTD, true))
		{
			parquetColumnarWriter.writeRowGroup(vals.length, rgw -> {
				rgw.writeValues(columnChunkWriter -> columnChunkWriter.writeColumn(
						messageType.getColumnDescription(new String[] { colName }),
						new NullableIterators.NullableIntegerIterator()
						{
							int readValues = -1;

							@Override
							public int getValue()
							{
								return vals[readValues];
							}

							@Override
							public boolean isNull()
							{
								return vals[readValues] == null;
							}

							@Override
							public boolean next()
							{
								return ++readValues < vals.length;
							}
						}));
			});
			parquetColumnarWriter.finishAndWriteFooterMetadata();
		}
	}
}
