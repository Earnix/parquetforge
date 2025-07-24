package com.earnix.parquet.columnar.file;

import com.earnix.parquet.columnar.file.reader.IndexedParquetColumnarFileReader;
import com.earnix.parquet.columnar.file.writer.ParquetFileColumnarWriterFactory;
import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkValuesReaderFactory;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkValuesReaderImpl;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import com.earnix.parquet.columnar.writer.columnchunk.NullableIterators;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
			writeValues(tmpFile, messageType, vals);

			IndexedParquetColumnarFileReader reader = new IndexedParquetColumnarFileReader(tmpFile);
			Assert.assertEquals(1, reader.getNumRowGroups());
			InMemChunk inMemChunk = reader.readInMem(0, messageType.getColumnDescription(new String[] { colName }));
			ChunkValuesReader chunkValuesReader = ChunkValuesReaderFactory.createChunkReader(inMemChunk);
			for (int i = 0; i < vals.length; i++)
			{
				Assert.assertEquals(vals[i] == null, chunkValuesReader.isNull());
				if (vals[i] != null)
					Assert.assertEquals((int) vals[i], chunkValuesReader.getInteger());
				Assert.assertEquals(i < vals.length - 1, chunkValuesReader.next());
			}
		}
		finally
		{
			Files.deleteIfExists(tmpFile);
		}
	}

	private static void writeValues(Path tmpFile, MessageType messageType, Integer[] vals) throws IOException
	{
		try (ParquetColumnarWriter parquetColumnarWriter = ParquetFileColumnarWriterFactory.createWriter(tmpFile,
				messageType, CompressionCodec.ZSTD, true))
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
