package com.earnix.parquet.columnar.writer.columnchunk;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import com.earnix.parquet.columnar.utils.ParquetEnumUtils;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;

/**
 * This class stores all the serialized pages for a specific column chunk within a row group.
 */
public class ColumnChunkPages extends ChunkPages
{
	private final ColumnDescriptor columnDescriptor;

	public ColumnChunkPages(ColumnDescriptor columnDescriptor, DictionaryPage dictionaryPage,
			List<? extends DataPage> dataPages)
	{
		super(dictionaryPage, dataPages);
		this.columnDescriptor = columnDescriptor;
	}

	public ColumnDescriptor getColumnDescriptor()
	{
		return columnDescriptor;
	}
}
