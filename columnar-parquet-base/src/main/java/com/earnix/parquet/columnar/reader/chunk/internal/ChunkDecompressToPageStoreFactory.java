package com.earnix.parquet.columnar.reader.chunk.internal;

import static com.earnix.parquet.columnar.utils.ParquetEnumUtils.convert;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.xerial.snappy.Snappy;

import com.github.luben.zstd.Zstd;

/**
 * A factory to read a column from an input stream into an uncompressed in memory page store.
 */
public class ChunkDecompressToPageStoreFactory
{
	private static final byte[] EMPTY = new byte[0];
	private static final Supplier<DictionaryPage> nullSupplier = () -> null;

	/**
	 * Build storage for a column chunk in memory
	 *
	 * @param descriptor           the descriptor of the column
	 * @param is                   the input stream to read the column from
	 * @param totalBytesInAllPages the total number of bytes in all of the column pages
	 * @param codec                the compression codec used for the pages
	 * @return the in memory page store
	 * @throws IOException on failure to read from the stream
	 */
	public static InMemChunkPageStore buildColumnChunkPageStore(ColumnDescriptor descriptor, CountingInputStream is,
			long totalBytesInAllPages, CompressionCodec codec) throws IOException
	{
		long endByte = is.getByteCount() + totalBytesInAllPages;
		boolean isFirstPage = true;
		Supplier<DictionaryPage> dictionaryPage = nullSupplier;
		List<Supplier<DataPage>> dataPageList = new ArrayList<>();
		long totalValues = 0L;
		while (is.getByteCount() < endByte)
		{
			PageHeader pageHeader = Util.readPageHeader(is);
			if (pageHeader.isSetDictionary_page_header())
			{
				if (!isFirstPage)
				{
					throw new IllegalStateException("Dict page only possible at beginning");
				}
				dictionaryPage = readDictPage(is, codec, pageHeader);
			}
			else if (pageHeader.isSetData_page_header_v2())
			{
				totalValues += pageHeader.getData_page_header_v2().getNum_values();
				dataPageList.add(handleDatapageV2(is, pageHeader, codec));
			}
			else if (pageHeader.isSetData_page_header())
			{
				totalValues += pageHeader.getData_page_header().getNum_values();
				dataPageList.add(handleDatapage(is, pageHeader, codec));
			}
			else
			{
				throw new IllegalStateException();
			}
			isFirstPage = false;
		}
		return new InMemChunkPageStore(descriptor, dictionaryPage, dataPageList, totalValues, totalBytesInAllPages);
	}

	private static Supplier<DictionaryPage> readDictPage(CountingInputStream is, CompressionCodec codec,
			PageHeader pageHeader) throws IOException
	{
		DictionaryPageHeader dictionaryPageHeader = pageHeader.getDictionary_page_header();
		byte[] compressedBytes = readPageFully(is, pageHeader.getCompressed_page_size());
		int uncompressedPageSize = pageHeader.getUncompressed_page_size();
		byte[] dictBytes = decompress(codec, compressedBytes, uncompressedPageSize);
		// decode dictionary!
		int numValues = dictionaryPageHeader.getNum_values();
		Encoding dictEncoding = convert(dictionaryPageHeader.getEncoding());
		return dictionaryPageSupplier(dictBytes, numValues, dictEncoding);
	}

	private static Supplier<DictionaryPage> dictionaryPageSupplier(byte[] dictBytes, int numValues,
			Encoding dictEncoding)
	{
		return () -> new DictionaryPage(wrap(dictBytes), numValues, dictEncoding);
	}

	private static Supplier<DataPage> handleDatapage(InputStream columnChunk, PageHeader pageHeader,
			CompressionCodec codec) throws IOException
	{
		DataPageHeader dataPageHeader = pageHeader.getData_page_header();

		byte[] dataBytesCompressed = readPageFully(columnChunk, pageHeader.getCompressed_page_size());
		byte[] dataBytesUncompressed = decompress(codec, dataBytesCompressed, pageHeader.getUncompressed_page_size());

		// TODO: statistics
		int numValues = dataPageHeader.getNum_values();
		Encoding repEncoding = convert(dataPageHeader.getRepetition_level_encoding());
		Encoding defEncoding = convert(dataPageHeader.getDefinition_level_encoding());
		Encoding dataEncoding = convert(dataPageHeader.getEncoding());
		return () -> dataPageV1Supplier(dataBytesUncompressed, numValues, repEncoding, defEncoding, dataEncoding);
	}

	private static DataPageV1 dataPageV1Supplier(byte[] dataBytesUncompressed, int numValues, Encoding repEncoding,
			Encoding defEncoding, Encoding dataEncoding)
	{
		return new DataPageV1(wrap(dataBytesUncompressed), numValues, dataBytesUncompressed.length, null, repEncoding,
				defEncoding, dataEncoding);
	}

	private static Supplier<DataPage> handleDatapageV2(InputStream columnChunk, PageHeader pageHeader,
			CompressionCodec codec) throws IOException
	{
		DataPageHeaderV2 dataPageHeaderV2 = pageHeader.getData_page_header_v2();

		byte[] repetitionLevelBytes = readPageFully(columnChunk, dataPageHeaderV2.getRepetition_levels_byte_length());
		byte[] defLevelBytes = readPageFully(columnChunk, dataPageHeaderV2.getDefinition_levels_byte_length());

		CompressionCodec usedCodec = dataPageHeaderV2.isIs_compressed() ? codec : CompressionCodec.UNCOMPRESSED;
		int defAndRepLen = dataPageHeaderV2.getDefinition_levels_byte_length()
				+ dataPageHeaderV2.getRepetition_levels_byte_length();
		int dataBytesCompressedLen = pageHeader.getCompressed_page_size() - defAndRepLen;
		int dataBytesUncompressedLen = pageHeader.getUncompressed_page_size() - defAndRepLen;
		byte[] dataBytesCompressed = readPageFully(columnChunk, dataBytesCompressedLen);
		byte[] dataBytesUncompressed = decompress(usedCodec, dataBytesCompressed, dataBytesUncompressedLen);

		int numRows = dataPageHeaderV2.getNum_rows();
		int numNulls = dataPageHeaderV2.getNum_nulls();
		int numValues = dataPageHeaderV2.getNum_values();
		Encoding encoding = convert(dataPageHeaderV2.getEncoding());
		return dataPageV2Supplier(numRows, numNulls, numValues, repetitionLevelBytes, defLevelBytes, encoding,
				dataBytesUncompressed);
	}

	private static Supplier<DataPage> dataPageV2Supplier(int numRows, int numNulls, int numValues,
			byte[] repetitionLevelBytes, byte[] defLevelBytes, Encoding encoding, byte[] dataBytesUncompressed)
	{
		return () -> DataPageV2.uncompressed(numRows, numNulls, numValues, wrap(repetitionLevelBytes),
				wrap(defLevelBytes), encoding, wrap(dataBytesUncompressed), null);
	}

	private static BytesInput wrap(byte[] bytes)
	{
		if (bytes == null)
			return null;
		if (bytes.length == 0)
			return BytesInput.empty();
		return BytesInput.from(bytes);
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
				long code = Zstd.decompress(uncompressed, compressedBytes);
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
