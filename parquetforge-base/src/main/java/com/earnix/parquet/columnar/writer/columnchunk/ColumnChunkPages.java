package com.earnix.parquet.columnar.writer.columnchunk;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.format.CompressionCodec;

import java.util.List;

/**
 * This class stores all the serialized pages for a specific column chunk within a row group.
 */
public class ColumnChunkPages extends ChunkPages
{
	private final ColumnDescriptor columnDescriptor;

	public ColumnChunkPages(ColumnDescriptor columnDescriptor, DictionaryPage dictionaryPage,
			List<? extends DataPage> dataPages, CompressionCodec compressionCodec)
	{
		super(dictionaryPage, dataPages, compressionCodec);
		this.columnDescriptor = columnDescriptor;
	}

	public ColumnDescriptor getColumnDescriptor()
	{
		return columnDescriptor;
	}
}
