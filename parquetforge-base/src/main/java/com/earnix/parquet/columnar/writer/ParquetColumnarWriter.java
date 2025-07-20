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

	/**
	 * Start a new row group with the specified number of rows
	 *
	 * @param numRows the number of rows of data in this row group
	 * @return the row group writer
	 * @throws IOException on failure to write to the destination
	 */
	RowGroupWriter startNewRowGroup(long numRows) throws IOException;

	/**
	 * @return the current row group writer that is opened for writing
	 */
	RowGroupWriter getCurrentRowGroupWriter();

	/**
	 * Mark the current row group as finished. This should also validate that all required columns are written when
	 * possible to prevent corrupted partial parquet files
	 */
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
