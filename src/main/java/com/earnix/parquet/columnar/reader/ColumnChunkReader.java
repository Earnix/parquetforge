package com.earnix.parquet.columnar.reader;

import static com.earnix.parquet.columnar.utils.ParquetEnumUtils.convert;

import org.apache.parquet.VersionParser;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.io.api.Binary;

public class ColumnChunkReader
{
	private final HackyParquetExtendedColumnReader columnReader;
	private final long numValues;
	private long numValuesRead;

	public ColumnChunkReader(ColumnDescriptor descriptor, Dictionary dictionary, PageHeader pageHeader,
			byte[] repetitionBytesUncompressed, byte[] definitionBytesUncompressed, byte[] dataPageBytesUncompressed)
	{
		PageReader pageReader = new PageReader()
		{
			@Override
			public DictionaryPage readDictionaryPage()
			{
				// dictionary is injected later so we can use the same dictionary between readers
				return null;
			}

			@Override
			public long getTotalValueCount()
			{
				if (pageHeader.isSetData_page_header_v2())
					return pageHeader.getData_page_header_v2().getNum_values();
				else if (pageHeader.isSetData_page_header())
					return pageHeader.getData_page_header().getNum_values();
				throw new IllegalStateException();
			}

			@Override
			public DataPage readPage()
			{
				if (pageHeader.isSetData_page_header_v2())
				{
					DataPageHeaderV2 header = pageHeader.getData_page_header_v2();
					return DataPageV2.uncompressed(header.getNum_rows(), header.getNum_nulls(), header.getNum_values(),
							wrap(repetitionBytesUncompressed), wrap(definitionBytesUncompressed),
							convert(header.getEncoding()), wrap(dataPageBytesUncompressed), null);
				}
				throw new IllegalStateException();
			}

			BytesInput wrap(byte[] bytes)
			{
				if (bytes == null)
					return null;
				return BytesInput.from(bytes);
			}
		};
		columnReader = new HackyParquetExtendedColumnReader(descriptor, pageReader, dictionary,
				new VersionParser.ParsedVersion("earnix", "0.0.1", "iliketacos"));
		numValues = numValues(pageHeader);
		numValuesRead = 0;
	}

	private static long numValues(PageHeader ph)
	{
		if (ph.isSetData_page_header_v2())
			return ph.getData_page_header_v2().getNum_values();
		if (ph.isSetData_page_header())
			return ph.getData_page_header().getNum_values();
		throw new IllegalStateException();
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
