package com.earnix.parquet.columnar.reader.chunk.internal;

import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;

import java.util.NoSuchElementException;

/**
 * Note that this class is *NOT* threadsafe
 */
public class ChunkValuesReaderImpl implements ChunkValuesReader
{
	private final FlatParquetColumnReader columnReader;
	private final long numValues;
	private long numValuesRead;

	public ChunkValuesReaderImpl(InMemChunkPageStore inMemChunkPageStore)
	{
		columnReader = new FlatParquetColumnReader(inMemChunkPageStore);
		numValues = inMemChunkPageStore.getTotalValues();
		numValuesRead = 0;
	}

	public ChunkValuesReaderImpl(InMemChunk chunk)
	{
		this(chunk, 0);
	}

	public ChunkValuesReaderImpl(InMemChunk chunk, int numRowsToSkip)
	{
		columnReader = FlatParquetColumnReader.createParquetExtendedColumnReader(chunk, numRowsToSkip);
		numValues = chunk.getTotalValues();
		numValuesRead = numRowsToSkip;
	}

	@Override
	public boolean isNull()
	{
		return columnReader.isNull();
	}

	@Override
	public boolean next()
	{
		if (++numValuesRead >= numValues)
		{
			return false;
		}

		columnReader.next();
		return true;
	}

	@Override
	public void skip(int rowsToSkip)
	{
		if (rowsToSkip < 0)
			throw new IllegalArgumentException("rowsToSkip " + rowsToSkip + " must be greater than or equal to 0");

		// how do we make this more efficient?
		for (int i = 0; i < rowsToSkip; i++)
		{
			if (!next())
			{
				throw new NoSuchElementException(
						"Failed to skip " + rowsToSkip + " elements, " + " the " + i + " element doest not exist");
			}
		}
	}

	@Override
	public int getInteger()
	{
		return columnReader.getInteger();
	}

	/**
	 * @return the current value
	 */
	@Override
	public boolean getBoolean()
	{
		return columnReader.getBoolean();
	}

	/**
	 * @return the current value
	 */
	@Override
	public long getLong()
	{
		return columnReader.getLong();
	}

	/**
	 * @return the current value
	 */
	@Override
	public Binary getBinary()
	{
		return columnReader.getBinary();
	}

	/**
	 * @return the current value
	 */
	@Override
	public float getFloat()
	{
		return columnReader.getFloat();
	}

	/**
	 * @return the current value
	 */
	@Override
	public double getDouble()
	{
		return columnReader.getDouble();
	}

	@Override
	public Dictionary getDictionary()
	{
		return columnReader.getDictionary();
	}

	@Override
	public boolean isDictionaryIdSupported()
	{
		return columnReader.currentPageUsesDictionary();
	}

	@Override
	public int getDictionaryId()
	{
		return columnReader.getCurrentValueDictionaryID();
	}
}
