package com.earnix.parquet.columnar.rowgroup;

import java.io.IOException;
import java.util.function.Function;

import com.earnix.parquet.columnar.columnchunk.ColumnChunkPages;
import com.earnix.parquet.columnar.columnchunk.ColumnChunkWriter;

public interface RowGroupWriter
{
	void writeColumn(Function<ColumnChunkWriter, ColumnChunkPages> writer) throws IOException;

	/**
	 * @return number of bytes written in this row.
	 */
	RowGroupInfo closeAndValidateAllColumnsWritten() throws IOException;
}
