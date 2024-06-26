package com.earnix.parquet.columnar;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.input.CountingInputStream;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;

public class UncompressedColumn
{

	public static UncompressedColumn build(ColumnMetaData columnMetaData, InputStream columnChunk) throws IOException
	{
		CountingInputStream wrappedIs = new CountingInputStream(columnChunk);

		DictionaryPageHeader dictionaryPageHeader;
		List<DataPageHeader> dataPageHeaders = new ArrayList<>();

		long bytesToRead = columnMetaData.getTotal_compressed_size();
		if (bytesToRead <= 0)
			throw new IllegalArgumentException(bytesToRead + " must be greater than zero");

		boolean isFirstPage = true;
		while (wrappedIs.getByteCount() < bytesToRead)
		{
			PageHeader pageHeader = Util.readPageHeader(columnChunk);
			if (pageHeader.isSetDictionary_page_header())
			{
				if (!isFirstPage)
				{
					throw new IllegalStateException("Dict page only possible at beginning");
				}
				dictionaryPageHeader = pageHeader.getDictionary_page_header();
			} else {

			}

			isFirstPage = false;
		}

		return null;
	}

	private ColumnReader makeColumnReader()
	{
		ColumnReader reader = null;
		return reader;
	}
}
