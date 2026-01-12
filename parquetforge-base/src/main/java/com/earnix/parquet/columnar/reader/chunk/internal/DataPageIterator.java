package com.earnix.parquet.columnar.reader.chunk.internal;

import org.apache.parquet.column.page.DataPage;
import shaded.parquet.it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Iterator;
import java.util.function.Supplier;

class DataPageIterator implements Iterator<DataPage>, ObjectIterator<DataPage>
{
	private final Iterator<Supplier<DataPage>> dataPageIterator;

	public DataPageIterator(Iterator<Supplier<DataPage>> dataPageIterator)
	{
		this.dataPageIterator = dataPageIterator;
	}

	@Override
	public boolean hasNext()
	{
		return dataPageIterator.hasNext();
	}

	@Override
	public DataPage next()
	{
		return dataPageIterator.next().get();
	}
}
