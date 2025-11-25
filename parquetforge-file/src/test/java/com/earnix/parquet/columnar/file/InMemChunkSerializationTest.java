package com.earnix.parquet.columnar.file;

import com.earnix.parquet.columnar.file.reader.ParquetFileReaderFactory;
import com.earnix.parquet.columnar.reader.IndexedParquetColumnarReader;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class InMemChunkSerializationTest
{
	@Test
	public void testInMemChunkSerialization() throws IOException
	{
		Path tmp = Files.createTempFile("parquetFile", ".parquet");
		try
		{
			ParquetFileFiller.basicSingleDoubleColumnParquetFile(tmp);
			IndexedParquetColumnarReader reader = ParquetFileReaderFactory.createIndexedColumnarFileReader(tmp);
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

	private static void assertEquals(IndexedParquetColumnarReader reader, int rowGroup) throws IOException
	{
		InMemChunk inMemChunk = reader.readInMem(rowGroup, reader.getDescriptor(0));
		InMemChunk copied = SerializationUtils.clone(inMemChunk);
		Assert.assertEquals(inMemChunk.getDescriptor(), copied.getDescriptor());
		ColEqualityCheckerUtils.assertDoubleColumnEqual(inMemChunk, copied);
	}
}
