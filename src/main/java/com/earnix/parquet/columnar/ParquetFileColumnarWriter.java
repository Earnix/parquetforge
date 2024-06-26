package com.earnix.parquet.columnar;

import java.io.Closeable;

public interface ParquetFileColumnarWriter extends Closeable
{
	RowGroupColumnarWriter startNewRowGroup(int numRows);

	void finishAndWriteFooterMetadata();
}
