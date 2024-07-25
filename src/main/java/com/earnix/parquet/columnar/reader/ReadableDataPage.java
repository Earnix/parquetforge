package com.earnix.parquet.columnar.reader;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.format.PageHeader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import static com.earnix.parquet.columnar.utils.ParquetEnumUtils.convert;

class ReadableDataPage
{
	private final ColumnDescriptor columnDescriptor;
	// the dictionary for this page. Null if it is not present
	private final Dictionary dictionary;

	// must be either data page or data page v2
	private final PageHeader pageHeader;
	private final byte[] definitionBytesUncompressed;
	private final byte[] repetitionBytesUncompressed;
	private final byte[] dataPageBytesUncompressed;

	public ReadableDataPage(ColumnDescriptor descriptor, Dictionary dictionary, PageHeader pageHeader,
			byte[] definitionBytesUncompressed, byte[] repetitionBytesUncompressed, byte[] dataPageBytesUncompressed)
	{
		this.columnDescriptor = descriptor;
		this.dictionary = dictionary;
		this.pageHeader = pageHeader;
		this.definitionBytesUncompressed = definitionBytesUncompressed;
		this.repetitionBytesUncompressed = repetitionBytesUncompressed;
		this.dataPageBytesUncompressed = dataPageBytesUncompressed;
	}

	/**
	 * Get the number of values in this column (including nulls)
	 * 
	 * @return the number of values in this column
	 */
	public int numValues()
	{
		if (pageHeader.isSetData_page_header_v2())
			return pageHeader.getData_page_header_v2().getNum_values();
		if (pageHeader.isSetData_page_header())
			return pageHeader.getData_page_header().getNum_values();
		throw new IllegalStateException("Invalid page header: " + pageHeader);
	}
	/**
	 * Get the number of values in this column (including nulls)
	 *
	 * @return the number of values in this column
	 */
	public int numNonNullValues()
	{
		if (pageHeader.isSetData_page_header_v2())
			return pageHeader.getData_page_header_v2().getNum_values()
					- pageHeader.getData_page_header_v2().getNum_nulls();
		if (pageHeader.isSetData_page_header())
			return pageHeader.getData_page_header().getNum_values();
		throw new IllegalStateException("Invalid page header: " + pageHeader);
	}


	public ValuesReader buildValuesReader()
	{
		ValuesReader reader = buildUninitializedValuesReader();

		try
		{
			// init values reader
			reader.initFromPage(numValues(),
					ByteBufferInputStream.wrap(ByteBuffer.wrap(this.dataPageBytesUncompressed).asReadOnlyBuffer()));
			return reader;
		}
		catch (IOException ex)
		{
			throw new UncheckedIOException(ex);
		}
	}

	public ValuesReader buildDefinitionLevelValuesReader()
	{
		// if the definition level is always zero, parquet omits it.
		if (columnDescriptor.getMaxDefinitionLevel() == 0)
		{
			return null;
		}
		//TODO
		return null;
	}

	private ValuesReader buildUninitializedValuesReader()
	{
		ValuesReader reader;
		Encoding encoding = getEncoding();
		if (encoding.usesDictionary())
		{
			if (dictionary == null)
			{
				throw new IllegalStateException("Dictionary cannot be null for " + encoding);
			}
			reader = encoding.getDictionaryBasedValuesReader(columnDescriptor, ValuesType.VALUES, dictionary);
		}
		else
		{
			reader = encoding.getValuesReader(columnDescriptor, ValuesType.VALUES);
		}
		return reader;
	}

	private Encoding getEncoding()
	{
		if (this.pageHeader.isSetData_page_header_v2())
			return convert(this.pageHeader.getData_page_header_v2().getEncoding());
		if (this.pageHeader.isSetData_page_header())
			return convert(this.pageHeader.getData_page_header().getEncoding());
		throw new IllegalStateException(String.valueOf(this.pageHeader));
	}
}
