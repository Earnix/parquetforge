package com.earnix.parquet.columnar.reader.processors;

import com.earnix.parquet.columnar.reader.chunk.InMemRowGroup;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.CompressionCodec;

import java.io.InputStream;

/**
 * A callback to process a parquet file chunk by chunk
 */
public class ParquetFileProcessors
{
	public interface ProcessPerChunk
	{
		void processChunk(InMemChunk chunk);
	}

	public interface ProcessPerRowGroup
	{
		void processChunk(InMemRowGroup rowGroup);
	}

	public interface ProcessRawChunkBytes
	{
		void processChunk(ColumnDescriptor descriptor, CompressionCodec codec, long rowOffset, long numValues,
				InputStream chunkInput, long numBytes);
	}
}
