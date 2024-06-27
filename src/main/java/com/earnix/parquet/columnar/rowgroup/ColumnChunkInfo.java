package com.earnix.parquet.columnar.rowgroup;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.Encoding;

import java.util.Set;

public class ColumnChunkInfo
{
	private final ColumnDescriptor descriptor;
	private final Set<Encoding> usedEncodings;
	private final long numValues;
	private final long startPos;
	private final long compressedLen;
	private final long uncompressedLen;

	public ColumnChunkInfo(ColumnDescriptor descriptor, Set<Encoding> usedEncs, long numValues, long startPos,
			long compressedLen, long uncompressedLen)
	{
		this.descriptor = descriptor;
		this.usedEncodings = usedEncs;
		this.numValues = numValues;
		this.startPos = startPos;
		this.compressedLen = compressedLen;
		this.uncompressedLen = uncompressedLen;
	}

	public ColumnDescriptor getDescriptor()
	{
		return descriptor;
	}

	public Set<Encoding> getUsedEncodings()
	{
		return usedEncodings;
	}

	public long getNumValues()
	{
		return numValues;
	}

	public long getStartPos()
	{
		return startPos;
	}

	public long getUncompressedLen()
	{
		return uncompressedLen;
	}

	public long getCompressedLen()
	{
		return compressedLen;
	}
}
