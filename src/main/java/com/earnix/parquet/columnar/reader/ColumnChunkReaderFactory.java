package com.earnix.parquet.columnar.reader;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;

public class ColumnChunkReaderFactory
{
	public static ColumnChunkReader build(ColumnDescriptor descriptor, Dictionary dictionary, InputStream is)
			throws IOException
	{
		PageHeader pageHeader = Util.readPageHeader(is);
		return build(descriptor, dictionary, pageHeader, is);
	}

	public static ColumnChunkReader build(ColumnDescriptor descriptor, Dictionary dictionary, PageHeader pageHeader,
			InputStream is) throws IOException
	{
		int pageSize = pageHeader.getCompressed_page_size();
		if (pageHeader.isSetData_page_header_v2())
		{
			DataPageHeaderV2 dataPageHeaderV2 = pageHeader.getData_page_header_v2();

			int chicken = dataPageHeaderV2.getRepetition_levels_byte_length();
			byte[] repBytes = readBytes(is, chicken);
			byte[] defBytes = readBytes(is, dataPageHeaderV2.getDefinition_levels_byte_length());
			byte[] dataBytes = readBytes(is, pageSize - repBytes.length - defBytes.length);

			return new ColumnChunkReader(descriptor, dictionary, pageHeader, repBytes, defBytes, dataBytes);
		}

		throw new IllegalStateException();
	}

	private static byte[] readBytes(InputStream is, int bytesToRead) throws IOException
	{
		byte[] repBytes = new byte[bytesToRead];
		IOUtils.readFully(is, repBytes);
		return repBytes;
	}
}
