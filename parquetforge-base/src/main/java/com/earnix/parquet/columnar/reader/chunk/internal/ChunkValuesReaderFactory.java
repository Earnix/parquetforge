package com.earnix.parquet.columnar.reader.chunk.internal;

import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;

public class ChunkValuesReaderFactory
{
	/**
	 * Create a chunk reader for the in mem chunk
	 *
	 * @param chunk the chunk data to read
	 * @return the chunk reader
	 */
	public static ChunkValuesReader createChunkReader(InMemChunk chunk)
	{
		return new ChunkValuesReaderImpl(chunk);
	}
}
