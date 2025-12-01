package com.earnix.parquet.columnar.reader;

import com.earnix.parquet.columnar.RowGroupRowIndex;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;

public interface BaseColumnarReader
{
	/**
	 * @return the number of row groups in this parquet file
	 * @throws IOException on failure to read footer metadata
	 */
	int getNumRowGroups() throws IOException;

	/**
	 * @param rowGroup the row group offset
	 * @return the number of rows in the specified row group
	 * @throws IOException on failure to read footer metadata
	 */
	long getNumRowsInRowGroup(int rowGroup) throws IOException;

	/**
	 * @return the total number of rows in all of the row groups
	 * @throws IOException on failure to read footer metadata
	 */
	long getTotalNumRows() throws IOException;

	/**
	 * @return an array containing the number of rows per row group.
	 */
	RowGroupRowIndex getRowGroupRowIndex() throws IOException;

	/**
	 * @return the schema of this parquet file.
	 */
	MessageType getMessageType();

	/**
	 * @return a list of the column descriptors present in this parquet file
	 */
	List<ColumnDescriptor> getColumnDescriptors() throws IOException;

	/**
	 * Get the column descriptor at the specified offset
	 *
	 * @param colOffset teh column offset (id)
	 * @return the ColumnDescriptor of this Column
	 * @throws IOException on failure to read footer metadata
	 */
	ColumnDescriptor getDescriptor(int colOffset) throws IOException;

	/**
	 * @return number of columns in this parquet file
	 * @throws IOException on failure to read footer metadata
	 */
	default int getNumColumns() throws IOException
	{
		return getColumnDescriptors().size();
	}
}
