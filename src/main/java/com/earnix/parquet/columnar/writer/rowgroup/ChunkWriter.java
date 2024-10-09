package com.earnix.parquet.columnar.writer.rowgroup;

import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkPages;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriter;

import java.util.function.Function;

@FunctionalInterface
public interface ChunkWriter extends Function<ColumnChunkWriter, ColumnChunkPages>
{
}
