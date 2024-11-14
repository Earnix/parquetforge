package com.earnix.parquet.columnar.writer.rowgroup;

import com.earnix.parquet.columnar.utils.ParquetEnumUtils;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkPages;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;

import java.util.ArrayList;
import java.util.Arrays;

public class PartialColumnChunkInfo extends ColumnChunkInfo
{
	private final ColumnChunkPages pages;
	private final CompressionCodec compressionCodec;

	public PartialColumnChunkInfo(ColumnChunkPages pages, long startPos, CompressionCodec compressionCodec)
	{
		super(startPos);
		this.pages = pages;
		this.compressionCodec = compressionCodec;
	}

	@Override
	public ColumnDescriptor getDescriptor()
	{
		return pages.getColumnDescriptor();
	}

	@Override
	public ColumnChunk buildChunkFromInfo()
	{
		ColumnChunk columnChunk = new ColumnChunk();
		columnChunk.setFile_offset(0);
		columnChunk.setMeta_data(getColumnMetaData());
		return columnChunk;
	}

	private ColumnMetaData getColumnMetaData()
	{
		ColumnMetaData columnMetaData = new ColumnMetaData();

		columnMetaData.setData_page_offset(getStartingOffset());
		columnMetaData.setTotal_compressed_size(pages.totalBytesForStorage());
		columnMetaData.setTotal_uncompressed_size(pages.getUncompressedBytes());
		columnMetaData.setNum_values(pages.getNumValues());

		columnMetaData.setPath_in_schema(Arrays.asList(getDescriptor().getPath()));
		columnMetaData.setType(ParquetEnumUtils.convert(getDescriptor().getPrimitiveType().getPrimitiveTypeName()));

		// the set of all encodings
		columnMetaData.setEncodings(new ArrayList<>(pages.getEncodingSet()));

		columnMetaData.setCodec(compressionCodec);
		return columnMetaData;
	}

	@Override
	public long getCompressedSize()
	{
		return pages.totalBytesForStorage();
	}

	@Override
	public long getUncompressedSize()
	{
		return pages.getUncompressedBytes();
	}
}
