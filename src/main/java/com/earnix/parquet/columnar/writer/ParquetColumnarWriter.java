package com.earnix.parquet.columnar.writer;

import java.io.Closeable;
import java.io.IOException;

import com.earnix.parquet.columnar.writer.rowgroup.RowGroupWriter;

public interface ParquetColumnarWriter extends Closeable
{
	RowGroupWriter startNewRowGroup(long numRows) throws IOException;

	void finishRowGroup() throws IOException;

	void finishAndWriteFooterMetadata() throws IOException;
}
