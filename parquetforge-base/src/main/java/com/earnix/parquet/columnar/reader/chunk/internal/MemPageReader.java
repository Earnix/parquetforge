package com.earnix.parquet.columnar.reader.chunk.internal;

import java.util.Iterator;

import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;

/**
 * Inspired by MemPageReader in parquet test project. Store pages in memory to be consumed by
 * {@link HackyParquetExtendedColumnReader}
 */
public class MemPageReader implements PageReader
{
	private final DictionaryPage dictionary;
	private final Iterator<? extends DataPage> dataPageIterator;
	private final long totalValueCount;

	public MemPageReader(DictionaryPage dp, Iterator<? extends DataPage> dataPageIterator, long totalValueCount)
	{
		this.dictionary = dp;
		this.dataPageIterator = dataPageIterator;
		this.totalValueCount = totalValueCount;
	}

	@Override
	public DictionaryPage readDictionaryPage()
	{
		return dictionary;
	}

	@Override
	public long getTotalValueCount()
	{
		return totalValueCount;
	}

	@Override
	public DataPage readPage()
	{
		if (dataPageIterator.hasNext())
			return dataPageIterator.next();
		return null;
	}
}
