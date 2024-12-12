package com.earnix.parquet.columnar.writer.rowgroup;

import org.apache.parquet.format.ColumnMetaData;

import java.util.List;

/**
 * Information about a Row Group and its placement within a parquet file
 */
public class RowGroupInfo
{
	private final long startingOffset;
	private final long compressedSize;
	private final long numRows;
	private final List<ColumnChunkInfo> cols;

	/**
	 * Construct a new row group info
	 *
	 * @param startingOffset the starting offset of the row group within the parquet file
	 * @param numRows        the number of rows within this row group
	 * @param cols           the chunk information for all columns within this row group
	 */
	public RowGroupInfo(long startingOffset, long numRows, List<ColumnChunkInfo> cols)
	{
		this(startingOffset, computeCompressedSize(cols), numRows, cols);
	}

	/**
	 * Construct a new row group info
	 *
	 * @param startingOffset the starting offset of the row group within the parquet file
	 * @param compressedSize the compressed size of this row group
	 * @param numRows        the number of rows within this row group
	 * @param cols           the chunk information for all columns within this row group
	 */
	public RowGroupInfo(long startingOffset, long compressedSize, long numRows, List<ColumnChunkInfo> cols)
	{
		this.startingOffset = startingOffset;
		this.compressedSize = compressedSize;
		this.numRows = numRows;
		this.cols = cols;
	}

	private static long computeCompressedSize(List<ColumnChunkInfo> infos)
	{
		return infos.stream().mapToLong(ColumnChunkInfo::getCompressedSize).sum();
	}

	/**
	 * The starting offset of this row group in the parquet file.
	 *
	 * @return the starting offset of this row group in the parquet
	 */
	public long getStartingOffset()
	{
		return startingOffset;
	}

	/**
	 * The total compressed size of this row group including page headers. See
	 * {@link ColumnMetaData#getTotal_compressed_size()}
	 *
	 * @return the total compressed size of this row group
	 */
	public long getCompressedSize()
	{
		return compressedSize;
	}

	/**
	 * The total uncompressed size of this row group including page headers. See {@link ColumnMetaData#getNum_values()}
	 *
	 * @return the total uncompressed size of this row group
	 */
	public long getUncompressedSize()
	{
		return cols.stream().mapToLong(ColumnChunkInfo::getUncompressedSize).sum();
	}

	/**
	 * Get the number of rows in this row group {@link ColumnMetaData#getNum_values()}
	 *
	 * @return the number of rows in this row group
	 */
	public long getNumRows()
	{
		return numRows;
	}

	/**
	 * The columns in this row group
	 *
	 * @return the columns in this row group
	 */
	public List<ColumnChunkInfo> getCols()
	{
		return cols;
	}
}
