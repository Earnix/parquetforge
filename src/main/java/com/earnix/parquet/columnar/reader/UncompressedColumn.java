package com.earnix.parquet.columnar.reader;

import java.util.List;

import org.apache.parquet.format.DictionaryPageHeader;

/**
 * A representation of a Parquet column in memory, stored decompressed for easy access
 */
public class UncompressedColumn
{
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

}
