package com.earnix.parquet.columnar.reader.chunk;

import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import org.apache.parquet.column.ColumnDescriptor;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Represents an entire row group stuffed into memory
 */
public class InMemRowGroup
{
	private final Map<ColumnDescriptor, InMemChunk> inMemChunkMap;
	private final long numRows;

	public InMemRowGroup(Map<ColumnDescriptor, InMemChunk> inMemChunkMap, long numRows)
	{
		this.inMemChunkMap = inMemChunkMap;
		this.numRows = numRows;
	}

	public void forEachColumnChunk(BiConsumer<ColumnDescriptor, InMemChunk> chunkProcessor)
	{
		inMemChunkMap.forEach(chunkProcessor);
	}

	public long getNumRows()
	{
		return numRows;
	}
}
