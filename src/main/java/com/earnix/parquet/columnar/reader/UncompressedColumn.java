package com.earnix.parquet.columnar.reader;

import java.util.List;

import org.apache.parquet.format.DictionaryPageHeader;

/**
 * A representation of a Parquet column in memory, stored decompressed for easy access
 */
public class UncompressedColumn
{
	private final List<ReadableDataPage> dataPageHeaderList;

	public UncompressedColumn(List<ReadableDataPage> dataPageHeaderList)
	{
		this.dataPageHeaderList = dataPageHeaderList;
	}

	public List<ReadableDataPage> getDataPageHeaderList()
	{
		return dataPageHeaderList;
	}
}
