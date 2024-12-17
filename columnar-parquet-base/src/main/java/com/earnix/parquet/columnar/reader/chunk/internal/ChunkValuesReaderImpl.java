package com.earnix.parquet.columnar.reader.chunk.internal;

import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import org.apache.parquet.io.api.Binary;

import java.util.NoSuchElementException;

/**
 * Note that this class is *NOT* threadsafe
 */
public class ChunkValuesReaderImpl implements ChunkValuesReader
{
	private final HackyParquetExtendedColumnReader columnReader;
	private final long numValues;
	private long numValuesRead;

	public ChunkValuesReaderImpl(InMemChunkPageStore inMemChunkPageStore)
	{
		columnReader = new HackyParquetExtendedColumnReader(inMemChunkPageStore);
		numValues = inMemChunkPageStore.getTotalValues();
		numValuesRead = 0;
	}

	public ChunkValuesReaderImpl(InMemChunk chunk)
	{
		columnReader = new HackyParquetExtendedColumnReader(chunk);
		numValues = chunk.getTotalValues();
		numValuesRead = 0;
	}

	@Override
	public boolean isNull()
	{
		return columnReader.getCurrentDefinitionLevel() < columnReader.getDescriptor().getMaxDefinitionLevel();
	}

	@Override
	public boolean next()
	{
		if (++numValuesRead >= numValues)
		{
			return false;
		}

		// skip is a misleading name for this method by the parquet people.
		// What it actually does it check to see if we read a value in the data column yet, and if not, we skip over
		// the current value in the data column
		columnReader.skip();
		columnReader.consume();
		return true;
	}

	@Override
	public void skip(int rowsToSkip)
	{
		if (rowsToSkip <= 0)
			throw new IllegalArgumentException("rowsToSkip " + rowsToSkip + " must be greater than 0");

		for (int ii = 0; ii < rowsToSkip; ii++)
		{
			if (!next())
			{
				throw new NoSuchElementException(
						"Failed to skip " + rowsToSkip + " elements, " + " the " + ii + " element doest not exist");
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
}
