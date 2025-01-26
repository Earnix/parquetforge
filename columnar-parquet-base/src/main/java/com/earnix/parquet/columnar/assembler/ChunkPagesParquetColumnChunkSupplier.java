package com.earnix.parquet.columnar.assembler;

import com.earnix.parquet.columnar.utils.ParquetMagicUtils;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkPages;
import com.earnix.parquet.columnar.writer.rowgroup.PartialColumnChunkInfo;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;

import java.io.IOException;
import java.io.InputStream;

/**
 * Supply chunk pages from generated in mem pages.
 */
public class ChunkPagesParquetColumnChunkSupplier implements ParquetColumnChunkSupplier
{
	private final ColumnChunkPages pages;

	public ChunkPagesParquetColumnChunkSupplier(ColumnChunkPages pages)
	{
		this.pages = pages;
	}

	@Override
	public long getNumRows()
	{
		return pages.getNumValues();
	}

	@Override
	public long getCompressedLength()
	{
		return pages.totalBytesForStorage();
	}

	@Override
	public ColumnChunk getColumnChunk()
	{
		return new PartialColumnChunkInfo(pages, ParquetMagicUtils.PARQUET_MAGIC.length()).buildChunkFromInfo();
	}

	@Override
	public InputStream openInputStream() throws IOException
	{
		return pages.toInputStream();
	}

	@Override
	public ColumnDescriptor getColumnDescriptor()
	{
		return pages.getColumnDescriptor();
	}
}
