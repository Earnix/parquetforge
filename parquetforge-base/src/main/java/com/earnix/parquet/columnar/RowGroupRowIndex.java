package com.earnix.parquet.columnar;

import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;

import java.util.Iterator;

/**
 * A util class to compute row group lengths and start offsets from Parquet Footer Metadata
 */
public class RowGroupRowIndex
{
	private final long[] rowStart;

	/**
	 * Compute row starts and lengths from the file metadata
	 *
	 * @param fileMetaData the footer metadata
	 */
	public RowGroupRowIndex(FileMetaData fileMetaData)
	{
		rowStart = new long[fileMetaData.getRow_groupsSize() + 1];
		Iterator<RowGroup> it = fileMetaData.getRow_groupsIterator();

		rowStart[0] = 0;
		for (int i = 0; it.hasNext(); i++)
		{
			RowGroup rowGroup = it.next();
			rowStart[i + 1] = rowStart[i] + rowGroup.getNum_rows();
		}

		// sanity check total num rows
		if (rowStart[rowStart.length - 1] != fileMetaData.getNum_rows())
		{
			throw new IllegalArgumentException(
					"Invalid parquet footer - row groups and total num rows is not consistent");
		}
	}

	/**
	 * Get the absolute start row offset of this row group;
	 *
	 * @param rowGroup the row group offset
	 * @return the row start of this row group
	 */
	public long getStartRow(int rowGroup)
	{
		assertRowGroupValid(rowGroup);
		return rowStart[rowGroup];
	}

	/**
	 * Get the number of rows in this row group;
	 *
	 * @param rowGroup the row group offset
	 * @return the number of rows in this row group
	 */
	public long getNumRows(int rowGroup)
	{
		assertRowGroupValid(rowGroup);
		return rowStart[rowGroup + 1] - rowStart[rowGroup];
	}

	/**
	 * @return the number of row groups
	 */
	public int getNumRowGroups()
	{
		return rowStart.length - 1;
	}

	/**
	 * @return a long array containing the number of rows in each row group.
	 */
	public long[] rowsPerRowGroup()
	{
		long[] ret = new long[getNumRowGroups()];
		for (int i = 0; i < ret.length; i++)
		{
			ret[i] = getNumRows(i);
		}
		return ret;
	}

	private void assertRowGroupValid(int rowGroup)
	{
		if (rowGroup < 0 || rowGroup >= getNumRowGroups())
			throw new IllegalArgumentException(
					"Row group out of range: " + rowGroup + " numRowGroups: " + getNumRowGroups());
	}
}
