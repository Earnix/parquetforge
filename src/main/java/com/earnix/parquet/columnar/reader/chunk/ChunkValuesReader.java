package com.earnix.parquet.columnar.reader.chunk;

import com.earnix.parquet.columnar.reader.chunk.internal.HackyParquetExtendedColumnReader;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunkPageStore;
import org.apache.parquet.io.api.Binary;

/**
 * Note that this class is *NOT* threadsafe
 */
public class ChunkValuesReader
{
	private final HackyParquetExtendedColumnReader columnReader;
	private final long numValues;
	private long numValuesRead;

	public ChunkValuesReader(InMemChunkPageStore inMemChunkPageStore)
	{
		columnReader = new HackyParquetExtendedColumnReader(inMemChunkPageStore);
		numValues = inMemChunkPageStore.getTotalValues();
		numValuesRead = 0;
	}

	public ChunkValuesReader(InMemChunk chunk)
	{
		columnReader = new HackyParquetExtendedColumnReader(chunk);
		numValues = chunk.getTotalValues();
		numValuesRead = 0;
	}

	public boolean isNull()
	{
		return columnReader.getCurrentDefinitionLevel() < columnReader.getDescriptor().getMaxDefinitionLevel();
	}

	public boolean next()
	{
		if (numValuesRead++ >= numValues)
		{
			return false;
		}
		columnReader.consume();
		return true;
	}

	public int getInteger()
	{
		return columnReader.getInteger();
	}

	/**
	 * @return the current value
	 */
	boolean getBoolean()
	{
		return columnReader.getBoolean();
	}

	/**
	 * @return the current value
	 */
	public long getLong()
	{
		return columnReader.getLong();
	}

	/**
	 * @return the current value
	 */
	public Binary getBinary()
	{
		return columnReader.getBinary();
	}

	/**
	 * @return the current value
	 */
	public float getFloat()
	{
		return columnReader.getFloat();
	}

	/**
	 * @return the current value
	 */
	public double getDouble()
	{
		return columnReader.getDouble();
	}
}
