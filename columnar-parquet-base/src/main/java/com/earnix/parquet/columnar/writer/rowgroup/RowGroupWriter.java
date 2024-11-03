package com.earnix.parquet.columnar.writer.rowgroup;

import java.io.IOException;
import java.io.InputStream;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;

public interface RowGroupWriter
{
	void writeValues(ChunkValuesWritingFunction writer) throws IOException;
	void writeCopyOfChunk(ColumnDescriptor columnDescriptor, ColumnChunk columnChunk, InputStream chunkInputStream);
}
