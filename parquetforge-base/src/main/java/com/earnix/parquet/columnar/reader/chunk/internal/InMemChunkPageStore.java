package com.earnix.parquet.columnar.reader.chunk.internal;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;

/**
 * An in memory store for pages in a chunk. This class stores suppliers for the pages so they can be safely used
 * multiple times, as BytesInput does not explicitly state in documentation that it is stateless and can be used
 * multiple times.
 */
public class InMemChunkPageStore
{
	private final ColumnDescriptor descriptor;
	private final Supplier<DictionaryPage> dictionaryPage;
	private final List<Supplier<DataPage>> dataPageList;
	private final long totalValues;
	private final long totalPageBytes;

	public InMemChunkPageStore(ColumnDescriptor descriptor, Supplier<DictionaryPage> dictionaryPage,
			List<Supplier<DataPage>> dataPageList, long totalValues, long totalPageBytes)
	{
		this.descriptor = descriptor;
		this.dictionaryPage = dictionaryPage;
		this.dataPageList = dataPageList;
		this.totalValues = totalValues;
		this.totalPageBytes = totalPageBytes;
	}

	public ColumnDescriptor getDescriptor()
	{
		return descriptor;
	}

	public Supplier<DictionaryPage> getDictionaryPage()
	{
		return dictionaryPage;
	}

	public List<Supplier<DataPage>> getDataPageList()
	{
		return dataPageList;
	}

	public long getTotalValues()
	{
		return totalValues;
	}

	public long getTotalPageBytes()
	{
		return totalPageBytes;
	}

	public MemPageReader toMemPageReader()
	{
		Iterator<Supplier<DataPage>> dataPageIterator = this.dataPageList.iterator();
		return new MemPageReader(dictionaryPage.get().decode(descriptor), new DataPageIterator(dataPageIterator),
				totalValues);
	}
}
