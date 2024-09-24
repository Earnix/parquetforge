package com.earnix.parquet.columnar.reader.chunk.internal;

import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * This class is a great example of how *not* to handle encapsulation. However, the alternative would be copy/pasting a
 * lot of code which is probably worse. <br>
 * 
 * This code allows us to construct a column reader impl using a shared dictionary - so multiple readers can read a
 * column without having the dict copied multiple places in memory
 */
public class HackyParquetExtendedColumnReader extends ColumnReaderImpl
{
	private static final DummyConverter dummyConverter = new DummyConverter();

	// a dummy parsed version - won't trigger corrupted file logic. Reading corrupted parquet files is a non goal.
	private static final VersionParser.ParsedVersion dummyParsedVersion = new VersionParser.ParsedVersion("earnix",
			"0.1", "????????");

	private boolean finishedConstructor = false;

	public HackyParquetExtendedColumnReader(InMemChunkPageStore inMemChunkPageStore)
	{
		this(inMemChunkPageStore.getDescriptor(), inMemChunkPageStore.toMemPageReader(), null, dummyParsedVersion);
	}

	public HackyParquetExtendedColumnReader(InMemChunk inMemChunk)
	{
		this(inMemChunk.getDescriptor(), inMemChunk.getDataPages(), inMemChunk.getDictionary(), dummyParsedVersion);
	}

	private HackyParquetExtendedColumnReader(ColumnDescriptor path, PageReader pageReader, Dictionary dictionary,
			VersionParser.ParsedVersion writerVersion)
	{
		super(path, pageReader, dummyConverter, writerVersion);
		setDictionary(dictionary);

		this.finishedConstructor = true;
	}

	private void setDictionary(Dictionary dictionary)
	{
		try
		{
			Class<? super ColumnReaderImpl> baseClass = ColumnReaderImpl.class.getSuperclass();
			Field field = baseClass.getDeclaredField("dictionary");
			field.setAccessible(true);

			// TODO: this does NOT work with Java 17/21. FIX THIS and find a better way (which stinks because it's hard)
			Field modifiersField = Field.class.getDeclaredField("modifiers");
			modifiersField.setAccessible(true);
			modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

			field.set(this, dictionary);
		}
		catch (Exception ex)
		{
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void consume()
	{
		if (finishedConstructor)
			super.consume();
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
	}
}
