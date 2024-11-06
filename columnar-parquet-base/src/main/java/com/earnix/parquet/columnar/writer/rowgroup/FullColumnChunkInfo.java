package com.earnix.parquet.columnar.writer.rowgroup;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;

public class FullColumnChunkInfo implements ColumnChunkInfo
{
	private final ColumnDescriptor descriptor;
	private final ColumnChunk columnChunk;
	private final long startPos;


	public FullColumnChunkInfo(ColumnDescriptor descriptor, ColumnChunk columnChunk, long startPos)
	{
		this.descriptor = descriptor;
		this.columnChunk = columnChunk;
		this.startPos = startPos;
	}

	@Override
	public ColumnDescriptor getDescriptor()
	{
		return descriptor;
	}

	@Override
	public ColumnChunk buildChunkFromInfo()
	{
		ColumnChunk columnChunkCopy = columnChunk.deepCopy();
		columnChunkCopy.getMeta_data().setData_page_offset(startPos);
		return columnChunkCopy;
	}

}
