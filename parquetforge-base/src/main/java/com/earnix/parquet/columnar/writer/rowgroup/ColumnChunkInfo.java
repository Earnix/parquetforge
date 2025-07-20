package com.earnix.parquet.columnar.writer.rowgroup;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;

/**
 * Metadata about a column chunk and its placement within a parquet file
 */
public abstract class ColumnChunkInfo
{
	private final long startingOffset;

	protected ColumnChunkInfo(long startingOffset)
	{
		this.startingOffset = startingOffset;
	}

	/**
	 * @return the constructed {@link ColumnChunk} for this column chunk
	 */
	public abstract ColumnChunk buildChunkFromInfo();

	/**
	 * @return the column descriptor for this chunk
	 */
	public abstract ColumnDescriptor getDescriptor();

	/**
	 * @return the offset of this column chunk within the parquet file
	 */
	public long getStartingOffset()
	{
		return startingOffset;
	}

	/**
	 * @return the compressed size of this chunk including the page headers. See:
	 *        {@link org.apache.parquet.format.ColumnMetaData#total_compressed_size}
	 */
	public abstract long getCompressedSize();

	/**
	 * @return the uncompressed size of this chunk including the page headers. See:
	 *        {@link org.apache.parquet.format.ColumnMetaData#total_uncompressed_size}
	 */
	public abstract long getUncompressedSize();
}
