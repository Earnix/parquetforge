package com.earnix.parquet.columnar.file;

import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkValuesReaderImpl;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import org.junit.Assert;

public class ColEqualityCheckerUtils
{
	static void assertDoubleColumnEqual(InMemChunk inMemChunk, InMemChunk copied)
	{
		ChunkValuesReader orig = new ChunkValuesReaderImpl(inMemChunk);
		ChunkValuesReader copy = new ChunkValuesReaderImpl(copied);

		long rowNum = 0;
		boolean next;
		do
		{

			Assert.assertEquals(orig.getDouble(), copy.getDouble(), 0d);
			boolean origNext = orig.next();
			boolean copyNext = copy.next();
			long failRow = rowNum;
			Assert.assertEquals("failed on row " + failRow, origNext, copyNext);
			next = origNext;
			rowNum++;
		}
		while (next);
	}
}
