package com.earnix.parquet.columnar.reader.chunk;

import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import org.apache.parquet.column.ColumnDescriptor;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents an entire row group stuffed into memory
 */
public class InMemRowGroup
{
	private final Map<ColumnDescriptor, InMemChunk> inMemChunkMap;
	private final long numRows;

	public InMemRowGroup(List<InMemChunk> inMemChunkMap, long numRows)
	{
		this.inMemChunkMap  =  toInMemChunkMap(inMemChunkMap);
		this.numRows = numRows;
	}

	public void forEachColumnChunk(Consumer<InMemChunk> chunkProcessor)
	{
		inMemChunkMap.values().forEach(chunkProcessor);
	}

	public void forEachColumnDescriptor(Collection<ColumnDescriptor> descriptorList, Consumer<InMemChunk> chunkProcessor)
	{
		inMemChunkMap.keySet().stream()
				.filter(descriptorList::contains)
				.map(inMemChunkMap::get)
				.forEach(chunkProcessor);
	}

	public long getNumRows()
	{
		return numRows;
	}

	private Map<ColumnDescriptor, InMemChunk> toInMemChunkMap(List<InMemChunk> inMemChunksForRowGroupProcessing)
	{
		return inMemChunksForRowGroupProcessing.stream()
				.collect(Collectors.toMap(InMemChunk::getDescriptor, Function.identity()));
	}

}
