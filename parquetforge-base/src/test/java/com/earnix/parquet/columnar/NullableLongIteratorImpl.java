package com.earnix.parquet.columnar;

import com.earnix.parquet.columnar.writer.columnchunk.NullableIterators;

public class NullableLongIteratorImpl implements NullableIterators.NullableLongIterator
{
	private int element = 0;

	@Override
	public long getValue()
	{
		return 1;
	}

	@Override
	public boolean isNull()
	{
		return element % 2 == 0;
	}

	@Override
	public boolean next()
	{
		return element++ < 2;
	}

	public void reset(){
		element = 0;
	}
}

