package com.earnix.parquet.columnar.reader;

import com.github.luben.zstd.Zstd;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class UncompressedColumnFactory
{

	private static final byte[] EMPTY = new byte[0];

	public static UncompressedColumn build(ColumnMetaData columnMetaData, InputStream columnChunk) throws IOException
	{
		CountingInputStream wrappedIs = new CountingInputStream(columnChunk);

		long bytesToRead = columnMetaData.getTotal_compressed_size();
		if (bytesToRead <= 0)
			throw new IllegalArgumentException(bytesToRead + " must be greater than zero");

		DictionaryPageHeader dictionaryPageHeader = null;
		byte[] dictBytes = null;

		List<ReadableDataPage> pages = new ArrayList<>();

		final CompressionCodec codec = columnMetaData.getCodec();
		boolean isFirstPage = true;
		while (wrappedIs.getByteCount() < bytesToRead)
		{
			PageHeader pageHeader = Util.readPageHeader(columnChunk);
			if (pageHeader.isSetDictionary_page_header())
			{
				if (!isFirstPage)
				{
					throw new IllegalStateException("Dict page only possible at beginning");
				}
				dictionaryPageHeader = pageHeader.getDictionary_page_header();
				byte[] compressedBytes = readPageFully(columnChunk, pageHeader.getCompressed_page_size());
				int uncompressedPageSize = pageHeader.getUncompressed_page_size();
				dictBytes = decompress(codec, compressedBytes, uncompressedPageSize);
			}
			else if (pageHeader.isSetData_page_header_v2())
			{
				handleDatapageV2(columnChunk, pageHeader, codec, pages);
			}

			isFirstPage = false;
		}

		return new UncompressedColumn(dictionaryPageHeader, dictBytes, pages);
	}

	private static void handleDatapageV2(InputStream columnChunk, PageHeader pageHeader, CompressionCodec codec,
			List<ReadableDataPage> pages) throws IOException
	{
		DataPageHeaderV2 dataPageHeaderV2 = pageHeader.getData_page_header_v2();

		byte[] defLevelBytes = readPageFully(columnChunk, dataPageHeaderV2.getDefinition_levels_byte_length());
		byte[] repetitionLevelBytes = readPageFully(columnChunk, dataPageHeaderV2.getRepetition_levels_byte_length());

		CompressionCodec usedCodec = dataPageHeaderV2.isIs_compressed() ? codec : CompressionCodec.UNCOMPRESSED;
		int defAndRepLen = dataPageHeaderV2.getDefinition_levels_byte_length()
				+ dataPageHeaderV2.getRepetition_levels_byte_length();
		int dataBytesCompressedLen = pageHeader.getCompressed_page_size() - defAndRepLen;
		int dataBytesUncompressedLen = pageHeader.getUncompressed_page_size() - defAndRepLen;
		byte[] dataBytesCompressed = readPageFully(columnChunk, dataBytesCompressedLen);
		byte[] dataBytesUncompressed = decompress(usedCodec, dataBytesCompressed, dataBytesUncompressedLen);

		ReadableDataPage readableDataPage = new ReadableDataPage(pageHeader, defLevelBytes, repetitionLevelBytes,
				dataBytesUncompressed);
		pages.add(readableDataPage);
	}

	private static byte[] decompress(CompressionCodec codec, byte[] compressedBytes, int uncompressedPageSize)
			throws IOException
	{
		byte[] uncompressed;
		switch (codec)
		{
			case SNAPPY:
			{
				uncompressed = Snappy.uncompress(compressedBytes);
			}
				break;
			case ZSTD:
			{
				uncompressed = new byte[uncompressedPageSize];
				long code = Zstd.decompress(compressedBytes, uncompressed);
				if (Zstd.isError(code))
					throw new IllegalStateException(Zstd.getErrorName(code));
			}
				break;
			case UNCOMPRESSED:
			{
				// nothing todo.
				uncompressed = compressedBytes;
			}
				break;
			default:
				throw new UnsupportedEncodingException("" + codec);
		}
		return uncompressed;
	}

	private static byte[] readPageFully(InputStream columnChunk, int bytesToRead) throws IOException
	{
		if (bytesToRead == 0)
			return EMPTY;
		byte[] compressedBytes = new byte[bytesToRead];
		IOUtils.readFully(columnChunk, compressedBytes);
		return compressedBytes;
	}

}
