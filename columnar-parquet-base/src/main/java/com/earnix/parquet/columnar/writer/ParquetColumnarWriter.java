package com.earnix.parquet.columnar.writer;

import com.earnix.parquet.columnar.writer.rowgroup.RowGroupWriter;

import java.io.Closeable;
import java.io.IOException;

public interface ParquetColumnarWriter extends Closeable
{
	/**
	 * Append a row group to the file.
	 *
	 * @param numRows          the number of rows in the row group
	 * @param rowGroupAppender The callback to get the RowGroupAppender. Passed as a callback to ensure that start and
	 *                         finish are called as expected
	 * @throws IOException on IO Failure
	 */
	default void writeRowGroup(long numRows, RowGroupAppender rowGroupAppender) throws IOException
	{
		RowGroupWriter rowGroupWriter = startNewRowGroup(numRows);
		rowGroupAppender.append(rowGroupWriter);
		finishRowGroup();
	}

	RowGroupWriter startNewRowGroup(long numRows) throws IOException;

	RowGroupWriter getCurrentRowGroupWriter();

	void finishRowGroup() throws IOException;


	/**
	 * Finish the parquet file. Writes the footer metadata. Note that {@link #close()} should still be called after.
	 */
	ParquetFileInfo finishAndWriteFooterMetadata() throws IOException;

	@FunctionalInterface
	interface RowGroupAppender
	{
		void append(RowGroupWriter rowGroupWriter) throws IOException;
	}
}
