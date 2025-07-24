package com.earnix.parquet.columnar.writer.rowgroup;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;

public class FullColumnChunkInfo extends ColumnChunkInfo
{
	private final ColumnDescriptor descriptor;
	private final ColumnChunk columnChunk;

	/**
	 * Construct chunk info from existing ColumnMetadata
	 *
	 * @param descriptor  the descriptor of the column
	 * @param columnChunk the existing metadata
	 * @param startPos    the start position in the destination Parquet file. It will be replaced in the chunk
	 *                    metadata.
	 */
	public FullColumnChunkInfo(ColumnDescriptor descriptor, ColumnChunk columnChunk, long startPos)
	{
		super(startPos);
		this.descriptor = descriptor;
		this.columnChunk = columnChunk.deepCopy();
	}

	@Override
	public ColumnDescriptor getDescriptor()
	{
		return descriptor;
	}

	@Override
	public ColumnChunk buildChunkFromInfo()
	{
		ColumnChunk columnChunkCopy = columnChunk.deepCopy();
		if (columnChunkCopy.getMeta_data().isSetDictionary_page_offset()
				&& columnChunkCopy.getMeta_data().getDictionary_page_offset() > 0)
		{
			int dictPageLen = Math.toIntExact(
					columnChunkCopy.getMeta_data().getData_page_offset() - columnChunkCopy.getMeta_data()
							.getDictionary_page_offset());
			columnChunkCopy.getMeta_data().setDictionary_page_offset(getStartingOffset());
			columnChunkCopy.getMeta_data().setData_page_offset(getStartingOffset() + dictPageLen);
		}
		else
		{
			columnChunkCopy.getMeta_data().setData_page_offset(getStartingOffset());
		}
		return columnChunkCopy;
	}

	@Override
	public long getCompressedSize()
	{
		return columnChunk.getMeta_data().getTotal_compressed_size();
	}

	@Override
	public long getUncompressedSize()
	{
		return columnChunk.getMeta_data().getTotal_uncompressed_size();
	}
}
