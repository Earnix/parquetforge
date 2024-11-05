package com.earnix.parquet.columnar.writer.rowgroup;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.Encoding;

import java.util.HashSet;
import java.util.Set;

public class FullColumnChunkInfo extends ColumnChunkInfo
{
	private final ColumnChunk columnChunk;

	public FullColumnChunkInfo(ColumnDescriptor descriptor, ColumnChunk columnChunk, long startPos)
	{
		super(descriptor, startPos);
		this.columnChunk = columnChunk;
	}

	@Override
	public Set<Encoding> getUsedEncodings()
	{
		return new HashSet<>(columnChunk.getMeta_data().getEncodings());
	}

	@Override
	public long getNumValues()
	{
		return columnChunk.getMeta_data().getNum_values();
	}

	@Override
	public long getUncompressedLen()
	{
		return columnChunk.getMeta_data().getTotal_uncompressed_size();
	}

	@Override
	public long getCompressedLen()
	{
		return columnChunk.getMeta_data().getTotal_compressed_size();
	}
}
