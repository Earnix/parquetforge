package com.earnix.parquet.columnar.reader;

import com.earnix.parquet.columnar.RowGroupRowIndex;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;

public interface BaseColumnarReader
{
	int getNumRowGroups() throws IOException;

	long getNumRowsInRowGroup(int rowGroup) throws IOException;

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

	ColumnDescriptor getDescriptor(int colOffset) throws IOException;
}
