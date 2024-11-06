package com.earnix.parquet.columnar.writer.rowgroup;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;

public interface ColumnChunkInfo
{
	ColumnChunk buildChunkFromInfo();
	ColumnDescriptor getDescriptor();

}
