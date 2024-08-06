package com.earnix.parquet.columnar.reader.chunk;

import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.CompressionCodec;

import java.io.InputStream;

/**
 * A callback to process a parquet file chunk by chunk
 */
public class ParquetFileProcessors
{
	interface ProcessPerChunk
	{
		void processChunk(InMemChunk chunk);
	}

	interface ProcessPerRowGroup
	{
		void processChunk(InMemRowGroup rowGroup);
	}

	interface ProcessRawChunkBytes
	{
		void processChunk(ColumnDescriptor descriptor, CompressionCodec codec, long rowOffset, long numValues,
				InputStream chunkInput, long numBytes);
	}
}
