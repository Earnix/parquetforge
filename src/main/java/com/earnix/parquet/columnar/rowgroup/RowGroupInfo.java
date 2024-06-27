package com.earnix.parquet.columnar.rowgroup;

import java.util.List;

public class RowGroupInfo
{
	private final long startingOffset;
	private final long compressedSize;
	private final long numRows;
	private final List<ColumnChunkInfo> cols;

	public RowGroupInfo(long startingOffset, long compressedSize, long numRows, List<ColumnChunkInfo> cols)
	{
		this.startingOffset = startingOffset;
		this.compressedSize = compressedSize;
		this.numRows = numRows;
		this.cols = cols;
	}

	public long getStartingOffset()
	{
		return startingOffset;
	}

	public long getCompressedSize()
	{
		return compressedSize;
	}
	public long getUncompressedSize()
	{
		return cols.stream().mapToLong(ColumnChunkInfo::getUncompressedLen).sum();
	}

	public long getNumRows()
	{
		return numRows;
	}

	public List<ColumnChunkInfo> getCols()
	{
		return cols;
	}
}
