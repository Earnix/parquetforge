package com.earnix.parquet.columnar.reader.chunk;

import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import org.apache.parquet.column.ColumnDescriptor;

import java.util.Map;

public class InMemRowGroup
{
	private final Map<ColumnDescriptor, InMemChunk> inMemChunkMap;
	private final long numRows;

	public InMemRowGroup(Map<ColumnDescriptor, InMemChunk> inMemChunkMap, long numRows)
	{
		this.inMemChunkMap = inMemChunkMap;
		this.numRows = numRows;
	}
}
