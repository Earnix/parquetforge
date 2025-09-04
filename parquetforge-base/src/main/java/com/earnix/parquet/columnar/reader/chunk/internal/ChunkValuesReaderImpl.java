package com.earnix.parquet.columnar.reader.chunk.internal;

import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import org.apache.parquet.io.api.Binary;

import java.util.NoSuchElementException;

/**
 * Note that this class is *NOT* threadsafe
 */
public class ChunkValuesReaderImpl implements ChunkValuesReader
{
	private final ParquetExtendedColumnReader columnReader;
	private final long numValues;
	private long numValuesRead;

	public ChunkValuesReaderImpl(InMemChunkPageStore inMemChunkPageStore)
	{
		columnReader = new ParquetExtendedColumnReader(inMemChunkPageStore);
		numValues = inMemChunkPageStore.getTotalValues();
		numValuesRead = 0;
	}

	public ChunkValuesReaderImpl(InMemChunk chunk)
	{
		columnReader = new ParquetExtendedColumnReader(chunk);
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

		// skip is a misleading name for this method in the parquet-java library
		// What it actually does it check to see if we read a value in the data column yet, and if not, we skip over
		// the next value in the data column. If a value was read, it is a noop.
		// If the value is null, we shouldn't skip the value because no value is stored for null
		if (!isNull())
			columnReader.skip();

		// consume is also misleading and the documentation is wrong. It consumes the Repetition and Definition level.
		// It does NOT consume the data value, which sometimes should NOT be consumed if it is null.
		columnReader.consume();
		return true;
	}

	@Override
	public void skip(int rowsToSkip)
	{
		if (rowsToSkip < 0)
			throw new IllegalArgumentException("rowsToSkip " + rowsToSkip + " must be greater than or equal to 0");

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
	public boolean isDictionaryIdSupported()
	{
		return columnReader.currentPageUsesDictionary();
	}

	/**
	 * Get the dictionary id of the current value if it is supported. Note that you MUST call
	 * {@link #isDictionaryIdSupported()} before each call to this. The parquet specification (as far as I can tell)
	 * does not require that all pages in a Column Chunk use the Dictionary if it is there. That means in theory a
	 * ColumnChunk could have a data page that is RLE
	 *
	 * @return whether the dictionary id is supported for this id
	 */
	@Override
	public int getDictionaryId()
	{
		return columnReader.getCurrentValueDictionaryID();
	}
}
