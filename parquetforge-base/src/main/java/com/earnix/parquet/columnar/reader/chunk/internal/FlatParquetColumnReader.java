package com.earnix.parquet.columnar.reader.chunk.internal;

import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

import java.util.Iterator;

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
public class FlatParquetColumnReader extends ColumnReaderImpl
{
	private static final DummyConverter dummyConverter = new DummyConverter();

	// a dummy parsed version - won't trigger corrupted file logic. Reading corrupted parquet files is a non goal.
	private static final VersionParser.ParsedVersion dummyParsedVersion = new VersionParser.ParsedVersion("earnix",
			"0.1", "????????");
	private final MemPageReader memPageReader;


	static FlatParquetColumnReader createParquetExtendedColumnReader(InMemChunk inMemChunk, final long rowsToSkip)
	{
		sanityCheckRowsToSkip(inMemChunk, rowsToSkip);

		Iterator<DataPage> dataPageIterator = inMemChunk.dataPageIterator();
		long skippedRows = 0;
		DataPage lastDataPage = null;
		while (skippedRows < rowsToSkip)
		{
			assertNextPageExists(inMemChunk, rowsToSkip, dataPageIterator, skippedRows);
			lastDataPage = dataPageIterator.next();
			int valueCount = lastDataPage.getValueCount();
			if (valueCount <= 0)
				throw new IllegalStateException("Page must have at least one value");

			skippedRows = Math.addExact(skippedRows, valueCount);
		}

		// unless we get lucky, and we start on a page boundary, we need to rewind, which the iterator does not support.
		// so instead concat our current page to the rest of the pages
		if (skippedRows > rowsToSkip)
		{
			if (null == lastDataPage)
				throw new IllegalStateException();
			dataPageIterator = prependLastDataPage(dataPageIterator, lastDataPage);
			skippedRows -= lastDataPage.getValueCount();
		}

		MemPageReader newMemPageReader = new MemPageReader(inMemChunk.getDataPages().getDictionary(), dataPageIterator,
				inMemChunk.getTotalValues() - skippedRows);
		var ret = new FlatParquetColumnReader(inMemChunk.getDescriptor(), newMemPageReader, dummyParsedVersion);

		for (long i = skippedRows; i < rowsToSkip; i++)
		{
			ret.next();
		}

		return ret;
	}

	private static Iterator<DataPage> prependLastDataPage(final Iterator<DataPage> dataPageIterator,
			DataPage lastDataPage)
	{
		return new Iterator<>()
		{
			private DataPage dataPage = lastDataPage;

			@Override
			public boolean hasNext()
			{
				return lastDataPage != null || dataPageIterator.hasNext();
			}

			@Override
			public DataPage next()
			{
				if (dataPage != null)
				{
					var ret = dataPage;
					dataPage = null;
					return ret;
				}
				return dataPageIterator.next();
			}
		};
	}

	private static void sanityCheckRowsToSkip(InMemChunk inMemChunk, long rowsToSkip)
	{
		if (rowsToSkip >= inMemChunk.getTotalValues())
		{
			throw new IllegalArgumentException(
					"rowsToSkip invalid " + inMemChunk.getDescriptor() + " " + rowsToSkip + " total rows: "
							+ inMemChunk.getTotalValues());
		}
	}

	private static void assertNextPageExists(InMemChunk inMemChunk, long rowsToSkip,
			Iterator<DataPage> dataPageIterator, long skippedRows)
	{
		if (!dataPageIterator.hasNext())
		{
			throw new IllegalStateException(
					"More pages expected. " + inMemChunk.getDescriptor() + " skippedRows: " + skippedRows
							+ " rowsToSkip:" + rowsToSkip);
		}
	}

	FlatParquetColumnReader(InMemChunkPageStore inMemChunkPageStore)
	{
		this(inMemChunkPageStore.getDescriptor(), inMemChunkPageStore.toMemPageReader(), dummyParsedVersion);
	}

	private FlatParquetColumnReader(ColumnDescriptor path, MemPageReader pageReader,
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

	/**
	 * @return the dictionary for this column or null if there is none. Note that a non-null value does NOT imply the
	 * 		current page uses the dictionary.
	 */
	public Dictionary getDictionary()
	{
		return memPageReader.getDictionary();
	}

	/**
	 * @return Whether we're sitting at a value that is null
	 */
	boolean isNull()
	{
		return this.getCurrentDefinitionLevel() < getDescriptor().getMaxDefinitionLevel();
	}

	/**
	 * Iterate to the next value or null. BEWARE - do not call this more times than {@link #getTotalValueCount()}
	 */
	void next()
	{
		// skip is a misleading name for this method in the parquet-java library
		// What it actually does it check to see if we read a value in the data column yet, and if not, we skip over
		// the next value in the data column. If a value was read, it is a noop.
		// If the value is null, we shouldn't skip the value because no value is stored for null
		if (!isNull())
			this.skip();

		// consume is also misleading and the documentation is wrong. It consumes the Repetition and Definition level.
		// It does NOT consume the data value, which sometimes should NOT be consumed if it is null.
		this.consume();
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
