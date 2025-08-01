package com.earnix.parquet.columnar.reader.processors;

import com.earnix.parquet.columnar.reader.chunk.InMemRowGroup;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;

import java.io.InputStream;

/**
 * A callback to process a parquet file column chunk by column chunk
 */
public class ParquetColumnarProcessors
{
	@FunctionalInterface
	public interface ChunkProcessor
	{
		void processChunk(InMemChunk chunk);
	}

	@FunctionalInterface
	public interface RowGroupProcessor
	{
		void processRowGroup(InMemRowGroup rowGroup);
	}

	@FunctionalInterface
	public interface ProcessRawChunkBytes
	{
		void processChunk(ColumnDescriptor descriptor, ColumnChunk columnChunk, InputStream chunkInput);
	}
}
