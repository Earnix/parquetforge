package com.earnix.parquet.columnar;

import com.earnix.parquet.columnar.columnchunk.ColumnChunkWriter;
import com.earnix.parquet.columnar.rowgroup.RowGroupWriter;

import java.io.Closeable;
import java.io.IOException;

public interface ParquetColumnarWriter extends Closeable
{
	RowGroupWriter startNewRowGroup(int numRows) throws IOException;

	void finishAndWriteFooterMetadata() throws IOException;
}
