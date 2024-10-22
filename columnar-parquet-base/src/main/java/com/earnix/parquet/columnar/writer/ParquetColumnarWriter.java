package com.earnix.parquet.columnar.writer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.earnix.parquet.columnar.writer.rowgroup.RowGroupWriter;

public interface ParquetColumnarWriter extends Closeable
{
	byte[] magicBytes = "PAR1".getBytes(StandardCharsets.US_ASCII);

	RowGroupWriter startNewRowGroup(long numRows) throws IOException;

	void finishRowGroup() throws IOException;

	void finishAndWriteFooterMetadata() throws IOException;
}
