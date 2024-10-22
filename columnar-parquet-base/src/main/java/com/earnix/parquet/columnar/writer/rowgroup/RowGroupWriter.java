package com.earnix.parquet.columnar.writer.rowgroup;

import java.io.IOException;
import java.util.function.Function;

import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkPages;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriter;

public interface RowGroupWriter
{
	void writeColumn(ChunkWriter writer) throws IOException;
}
