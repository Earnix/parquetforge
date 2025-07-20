package com.earnix.parquet.columnar.assembler;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;

import java.io.IOException;
import java.io.InputStream;

/**
 * Represents all the information to put this column chunk within a parquet file
 */
public interface ParquetColumnChunkSupplier
{
	/**
	 * @return the number of rows in this column chunk
	 */
	long getNumRows();

	/**
	 * @return the compressed length of this column chunk including page headers
	 */
	long getCompressedLength();

	/**
	 * @return the column chunk metadata for this column. Note that this creates a deep copy of the metadata every time
	 * 		it is called to ensure that the metadata is not accidentally mutated, so avoid muliple repetitive calls
	 */
	ColumnChunk getColumnChunk();

	/**
	 * @return an input stream for the bytes within this chunk
	 * @throws IOException on failure
	 */
	InputStream openInputStream() throws IOException;

	/**
	 * @return the column descriptor for this column
	 */
	ColumnDescriptor getColumnDescriptor();
}
