package com.earnix.parquet.columnar.reader.chunk.internal;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

import java.lang.reflect.Field;

/**
 * This class is a great example of how *not* to handle encapsulation. However, the alternative would be copy/pasting a
 * lot of code which is probably worse. <br>
 * <p>
 * This class shouldn't be directly used. Use {@link ChunkValuesReaderImpl} instead
 * </p>
 * <p>
 * This code allows us to construct a column reader impl using a shared dictionary - so multiple readers can read a
 * column without having the dict copied multiple places in memory
 * </p>
 */
public class ParquetExtendedColumnReader extends ColumnReaderImpl
{
	private static final DummyConverter dummyConverter = new DummyConverter();

	// a dummy parsed version - won't trigger corrupted file logic. Reading corrupted parquet files is a non goal.
	private static final VersionParser.ParsedVersion dummyParsedVersion = new VersionParser.ParsedVersion("earnix",
			"0.1", "????????");
	private final MemPageReader memPageReader;

	ParquetExtendedColumnReader(InMemChunkPageStore inMemChunkPageStore)
	{
		this(inMemChunkPageStore.getDescriptor(), inMemChunkPageStore.toMemPageReader(), dummyParsedVersion);
	}

	ParquetExtendedColumnReader(InMemChunk inMemChunk)
	{
		this(inMemChunk.getDescriptor(), inMemChunk.getDataPages(), dummyParsedVersion);
	}

	private ParquetExtendedColumnReader(ColumnDescriptor path, MemPageReader pageReader,
			VersionParser.ParsedVersion writerVersion)
	{
		super(path, pageReader, dummyConverter, writerVersion);
		this.memPageReader = pageReader;
	}

	/**
	 * @return whether the current page being read uses a dictionary encoding. This must not be called after all values
	 * 		are read.
	 */
	public boolean currentPageUsesDictionary()
	{
		return memPageReader.getValuesEncoding().usesDictionary();
	}

	private static class DummyConverter extends PrimitiveConverter
	{
		@Override
		public void addBinary(Binary value)
		{
		}

		@Override
		public void addBoolean(boolean value)
		{
		}

		@Override
		public void addDouble(double value)
		{
		}

		@Override
		public void addFloat(float value)
		{
		}

		@Override
		public void addInt(int value)
		{
		}

		@Override
		public void addLong(long value)
		{
		}

		@Override
		public boolean hasDictionarySupport()
		{
			// support dictionary otherwise getCurrentValueDictionaryID() will not work
			return true;
		}

		@Override
		public void setDictionary(Dictionary dictionary)
		{
			//ignore
		}

		@Override
		public void addValueFromDictionary(int dictionaryId)
		{
			// ignore
		}
	}
}
