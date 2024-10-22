package com.earnix.parquet.columnar.reader.chunk.internal;


import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * Contains everything necessary for decoding a column without duplication of column data
 */
public class InMemChunk
{
	private final ColumnDescriptor descriptor;
	private final Dictionary dictionary;
	private final List<Supplier<DataPage>> dataPages;
	private final long totalValues;
	private final long totalPageBytes;

	public InMemChunk(InMemChunkPageStore pageStore) throws IOException
	{
		this.descriptor = pageStore.getDescriptor();
		DictionaryPage dictionaryPage = pageStore.getDictionaryPage().get();
		this.dictionary = readDictionary(dictionaryPage);
		this.dataPages = pageStore.getDataPageList();
		this.totalValues = pageStore.getTotalValues();
		this.totalPageBytes = pageStore.getTotalPageBytes();
	}

	private Dictionary readDictionary(DictionaryPage dp) throws IOException
	{
		if (dp == null)
			return null;
		return dp.getEncoding().initDictionary(descriptor, dp);
	}

	public ColumnDescriptor getDescriptor()
	{
		return descriptor;
	}

	public Dictionary getDictionary()
	{
		return dictionary;
	}

	public PageReader getDataPages()
	{
		return new MemPageReader(null, new DataPageIterator(this.dataPages.iterator()), getTotalValues());
	}

	public long getTotalValues()
	{
		return totalValues;
	}

	/**
	 * A rough estimate of the memory footprint of this column chunk
	 * 
	 * @return a rough estimate of the memory footprint of this column chunk
	 */
	public long estimatedMemoryFootprint()
	{
		return 100L + 100L * dataPages.size() + this.totalPageBytes;
	}
}