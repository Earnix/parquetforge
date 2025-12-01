package com.earnix.parquet.columnar.writer.rowgroup;

import java.io.IOException;
import java.io.InputStream;

import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkPages;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;

public interface RowGroupWriter
{
	/**
	 * Write column data as primitive values
	 *
	 * @param writer a function that passes values into the writer which then returns pages
	 * @throws IOException on failure
	 */
	void writeValues(ChunkValuesWritingFunction writer) throws IOException;

	void writeValues(ColumnChunkPages columnChunkPages) throws IOException;

	void writeCopyOfChunk(ColumnDescriptor columnDescriptor, ColumnChunk columnChunk, InputStream chunkInputStream);
}
