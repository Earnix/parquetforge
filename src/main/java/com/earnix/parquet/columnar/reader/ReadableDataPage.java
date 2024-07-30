package com.earnix.parquet.columnar.reader;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.format.PageHeader;

public class ReadableDataPage
{
	private final ColumnDescriptor columnDescriptor;
	// the dictionary for this page. Null if it is not present
	private final Dictionary dictionary;

	// must be either data page or data page v2
	private final PageHeader pageHeader;
	private final byte[] repetitionBytesUncompressed;
	private final byte[] definitionBytesUncompressed;
	private final byte[] dataPageBytesUncompressed;

	public ReadableDataPage(ColumnDescriptor descriptor, Dictionary dictionary, PageHeader pageHeader,
			byte[] repetitionBytesUncompressed, byte[] definitionBytesUncompressed, byte[] dataPageBytesUncompressed)
	{
		this.columnDescriptor = descriptor;
		this.dictionary = dictionary;
		this.pageHeader = pageHeader;
		this.definitionBytesUncompressed = definitionBytesUncompressed;
		this.repetitionBytesUncompressed = repetitionBytesUncompressed;
		this.dataPageBytesUncompressed = dataPageBytesUncompressed;
	}

	public ColumnChunkReader buildReader()
	{
		return new ColumnChunkReader(columnDescriptor, dictionary, pageHeader, repetitionBytesUncompressed,
				definitionBytesUncompressed, dataPageBytesUncompressed);
	}
}
