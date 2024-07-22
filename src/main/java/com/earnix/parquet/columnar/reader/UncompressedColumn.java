package com.earnix.parquet.columnar.reader;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.input.CountingInputStream;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;

/**
 * A representation of a Parquet column in memory, stored decompressed for easy access
 */
public class UncompressedColumn
{
	public static UncompressedColumn build(ColumnMetaData columnMetaData, InputStream columnChunk) throws IOException
	{
		CountingInputStream wrappedIs = new CountingInputStream(columnChunk);


		long bytesToRead = columnMetaData.getTotal_compressed_size();
		if (bytesToRead <= 0)
			throw new IllegalArgumentException(bytesToRead + " must be greater than zero");

		DictionaryPageHeader dictionaryPageHeader;
		byte[] dictBytes;

		List<ReadableDataPage> pages = new ArrayList<>();

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
				switch (columnMetaData.getCodec())
				{
					case SNAPPY:
					case ZSTD:
					case UNCOMPRESSED:
					{
						//dictBytes = wrappedIs.readNBytes(pageHeader.getCompressed_page_size());
					}
						break;
					default:
						throw new UnsupportedEncodingException("" + columnMetaData.getCodec());
				}
			}
			else
			{

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

	private final DictionaryPageHeader dictionaryPageHeader;
	private final byte[] dictBytes;

	private final List<ReadableDataPage> dataPageHeaderList;

	public UncompressedColumn(DictionaryPageHeader dictionaryPageHeader, byte[] dictBytes,
			List<ReadableDataPage> dataPageHeaderList)
	{
		this.dictionaryPageHeader = dictionaryPageHeader;
		this.dictBytes = dictBytes;
		this.dataPageHeaderList = dataPageHeaderList;
	}

	static class ReadableDataPage
	{
		// must be either data page or data page v2
		private final PageHeader pageHeader;
		private final byte[] definitionBytesUncompressed;
		private final byte[] repetitionBytesUncompressed;
		private final byte[] dataPageBytesUncompressed;

		public ReadableDataPage(PageHeader pageHeader, byte[] definitionBytesUncompressed,
				byte[] repetitionBytesUncompressed, byte[] dataPageBytesUncompressed)
		{
			this.pageHeader = pageHeader;
			this.definitionBytesUncompressed = definitionBytesUncompressed;
			this.repetitionBytesUncompressed = repetitionBytesUncompressed;
			this.dataPageBytesUncompressed = dataPageBytesUncompressed;
		}
	}
}
