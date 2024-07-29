package com.earnix.parquet.columnar.reader;

import static com.earnix.parquet.columnar.utils.ParquetEnumUtils.convert;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;

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

	public ColumnChunkReader buildValuesReader()
	{
		if (pageHeader.isSetData_page_header_v2())
		{
			DataPageHeaderV2 dataPageHeaderV2 = pageHeader.getData_page_header_v2();
			// note - statistics are not supported.
			Statistics<?> statistics = null;
			DataPageV2 dataPageV2 = new DataPageV2(dataPageHeaderV2.getNum_rows(), dataPageHeaderV2.getNum_nulls(),
					dataPageHeaderV2.getNum_values(), BytesInput.from(repetitionBytesUncompressed),
					BytesInput.from(definitionBytesUncompressed), getEncoding(),
					BytesInput.from(dataPageBytesUncompressed), pageHeader.getUncompressed_page_size(), statistics,
					false);
			ColumnChunkReader reader = new ColumnChunkReader(columnDescriptor, dictionary, pageHeader,
					repetitionBytesUncompressed, definitionBytesUncompressed, dataPageBytesUncompressed);
		}
		throw new IllegalStateException();
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
