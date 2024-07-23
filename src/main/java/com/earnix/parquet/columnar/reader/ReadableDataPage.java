package com.earnix.parquet.columnar.reader;

import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.format.PageHeader;

class ReadableDataPage
{
	// must be either data page or data page v2
	private final PageHeader pageHeader;
	private final byte[] definitionBytesUncompressed;
	private final byte[] repetitionBytesUncompressed;
	private final byte[] dataPageBytesUncompressed;

	public ReadableDataPage(PageHeader pageHeader, byte[] definitionBytesUncompressed,
			byte[] repetitionBytesUncompressed, byte[] dataPageBytesUncompressed)
	{
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

	public ValuesReader getReader()
	{
		return null;
	}

}
