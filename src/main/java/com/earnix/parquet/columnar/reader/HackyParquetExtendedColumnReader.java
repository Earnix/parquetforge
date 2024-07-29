package com.earnix.parquet.columnar.reader;

import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.PrimitiveConverter;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class HackyParquetExtendedColumnReader extends ColumnReaderImpl
{
	private boolean finishedConstructor = false;

	public HackyParquetExtendedColumnReader(ColumnDescriptor path, PageReader pageReader, Dictionary dictionary,
			VersionParser.ParsedVersion writerVersion)
	{
		super(path, pageReader, dummyConverter(), writerVersion);
		setDictionary(dictionary);

		this.finishedConstructor = true;
	}

	private static PrimitiveConverter dummyConverter()
	{
		return new PrimitiveConverter()
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
		};
	}

	private void setDictionary(Dictionary dictionary)
	{
		try
		{
			Class<? super ColumnReaderImpl> baseClass = ColumnReaderImpl.class.getSuperclass();
			Field field = baseClass.getDeclaredField("dictionary");
			field.setAccessible(true);

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
}
