package com.earnix.parquet.columnar.reader.chunk.internal;

import java.util.Iterator;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;

/**
 * Inspired by MemPageReader in parquet test project. Store pages in memory to be consumed by
 * {@link ParquetExtendedColumnReader}
 */
public class MemPageReader implements PageReader
{
	private final Dictionary dictionary;
	private final Iterator<? extends DataPage> dataPageIterator;
	private final long totalValueCount;
	private DataPage currentPage;

	/**
	 * Construct a new in memory page reader
	 *
	 * @param dictionary       the dictionary if used by any datapages, or null if not
	 * @param dataPageIterator the iterator for the DataPages (supports both V1 and V2)
	 * @param totalValueCount  the total number of values
	 */
	public MemPageReader(Dictionary dictionary, Iterator<? extends DataPage> dataPageIterator, long totalValueCount)
	{
		this.dictionary = dictionary;
		this.dataPageIterator = dataPageIterator;
		this.totalValueCount = totalValueCount;
	}

	@Override
	public DictionaryPage readDictionaryPage()
	{
		if (dictionary == null)
			return null;
		return new DictionaryPage(BytesInput.empty(), dictionary.getMaxId(), dictionary.getEncoding())
		{
			@Override
			public Dictionary decode(ColumnDescriptor path)
			{
				return dictionary;
			}
		};
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
		{
			currentPage = dataPageIterator.next();
			return currentPage;
		}
		else
		{
			currentPage = null;
		}
		return null;
	}

	/**
	 * @return the encoding of the values in the last read DataPage
	 */
	public Encoding getValuesEncoding()
	{
		if (currentPage != null)
		{
			if (currentPage instanceof DataPageV2)
				return ((DataPageV2) currentPage).getDataEncoding();
			if (currentPage instanceof DataPageV1)
				return ((DataPageV1) currentPage).getValueEncoding();
			throw new IllegalStateException("Unknown data page class: " + currentPage.getClass());
		}
		return null; // we're not on the first page yet.
	}
}
