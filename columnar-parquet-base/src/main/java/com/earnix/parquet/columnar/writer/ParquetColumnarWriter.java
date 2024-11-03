package com.earnix.parquet.columnar.writer;

import com.earnix.parquet.columnar.writer.rowgroup.RowGroupWriter;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;

public interface ParquetColumnarWriter extends Closeable
{
	void finishAndWriteFooterMetadata() throws IOException;

	void processRowGroup(long numRows, Consumer<RowGroupWriter> rowGroupAppender);

}
