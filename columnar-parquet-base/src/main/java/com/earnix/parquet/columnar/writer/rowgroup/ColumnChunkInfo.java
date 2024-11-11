package com.earnix.parquet.columnar.writer.rowgroup;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;

public abstract class ColumnChunkInfo
{
	private final long startingOffset;

	protected ColumnChunkInfo(long startingOffset)
	{
		this.startingOffset = startingOffset;
	}

	public abstract ColumnChunk buildChunkFromInfo();

	public abstract ColumnDescriptor getDescriptor();

	public long getStartingOffset()
	{
		return startingOffset;
	}

	public abstract long getCompressedSize();
}
