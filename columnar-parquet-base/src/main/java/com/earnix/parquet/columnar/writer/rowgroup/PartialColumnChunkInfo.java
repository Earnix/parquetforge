package com.earnix.parquet.columnar.writer.rowgroup;

import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkPages;
import org.apache.parquet.format.Encoding;

import java.util.Set;

public class PartialColumnChunkInfo extends ColumnChunkInfo
{
	private final ColumnChunkPages pages;

	public PartialColumnChunkInfo(ColumnChunkPages pages, long startPos)
	{
		super(pages.getColumnDescriptor(), startPos);
		this.pages = pages;
	}

	@Override
	public Set<Encoding> getUsedEncodings()
	{
		return pages.getEncodingSet();
	}

	@Override
	public long getNumValues()
	{
		return pages.getNumValues();
	}


	@Override
	public long getUncompressedLen()
	{
		return pages.getUncompressedBytes();
	}

	@Override
	public long getCompressedLen()
	{
		return pages.totalBytesForStorage();
	}
}
