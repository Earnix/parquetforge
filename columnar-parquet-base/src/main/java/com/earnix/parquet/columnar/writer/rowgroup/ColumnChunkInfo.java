package com.earnix.parquet.columnar.writer.rowgroup;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.Encoding;

import java.util.Set;

public abstract class ColumnChunkInfo
{
	private final ColumnDescriptor descriptor;
	private final long startPos;

	protected ColumnChunkInfo(ColumnDescriptor descriptor,  long startPos)
	{
		this.descriptor = descriptor;
		this.startPos = startPos;
	}

	public abstract  long getUncompressedLen();
	public abstract  long getCompressedLen();
	public abstract  Set<Encoding> getUsedEncodings();
	public abstract  long getNumValues();

	public ColumnDescriptor getDescriptor()
	{
		return descriptor;
	}

	public long getStartPos()
	{
		return startPos;
	}
}
