package com.earnix.parquet.columnar.writer.rowgroup;

import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkPages;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriter;

import java.io.IOException;
import java.util.function.Function;

@FunctionalInterface
public interface ChunkValuesWritingFunction
{
	ColumnChunkPages apply(ColumnChunkWriter columnChunkWriter) throws IOException;
}
