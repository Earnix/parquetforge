package com.earnix.parquet.columnar.columnchunk;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;

/**
 * This class stores all the pages for a specific column chunk.
 */
public class ColumnChunkPages
{
	private final ColumnDescriptor columnDescriptor;
	private final Set<Encoding> encodingSet = EnumSet.noneOf(Encoding.class);
	private final List<byte[]> headersAndPages;
	private final long numValues;
	private final long uncompressedBytes;
	private final long compressedBytes;

	public ColumnChunkPages(ColumnDescriptor columnDescriptor, DictionaryPage dictionaryPage,
			List<? extends DataPage> dataPages)
	{
		this.columnDescriptor = columnDescriptor;
		int numPages = dictionaryPage == null ? dataPages.size() : dataPages.size() + 1;
		this.headersAndPages = new ArrayList<>(2 * numPages);
		long uncompressedBytes = 0;
		if (dictionaryPage != null)
		{
			DictionaryPageHeader dictionaryPageHeader = new DictionaryPageHeader();
			Encoding enc = convert(dictionaryPage.getEncoding());
			dictionaryPageHeader.setEncoding(enc);
			encodingSet.add(enc);
			dictionaryPageHeader.setIs_sorted(false); // maybe it sorts it ?? Who knows, but lets not assume
			dictionaryPageHeader.setNum_values(dictionaryPage.getDictionarySize());

			PageHeader pageHeader = new PageHeader();
			pageHeader.setType(PageType.DICTIONARY_PAGE);
			pageHeader.setDictionary_page_header(dictionaryPageHeader);

			pageHeader.setUncompressed_page_size(dictionaryPage.getUncompressedSize());
			pageHeader.setCompressed_page_size(dictionaryPage.getCompressedSize());

			uncompressedBytes += storeHeaderBytes(pageHeader);
			uncompressedBytes += dictionaryPage.getUncompressedSize();
			addBytes(dictionaryPage.getBytes());
		}

		long numValues = 0;
		for (DataPage abstractDataPage : dataPages)
		{
			if (abstractDataPage instanceof DataPageV2)
			{
				uncompressedBytes += addPage((DataPageV2) abstractDataPage);
				numValues += abstractDataPage.getValueCount();
			}
			else
			{
				// shouldn't happen.
				throw new IllegalStateException();
			}
		}
		this.uncompressedBytes = uncompressedBytes;
		this.compressedBytes = this.headersAndPages.stream().mapToLong(Array::getLength).sum();
		this.numValues = numValues;
	}

	public long compressedBytes()
	{
		return compressedBytes;
	}

	public long getUncompressedBytes()
	{
		return uncompressedBytes;
	}

	public long getNumValues()
	{
		return numValues;
	}

	public void writeToOutputStream(FileChannel fc, long startingOffset) throws IOException
	{
		long offset = startingOffset;
		for (byte[] b : this.headersAndPages)
		{
			ByteBuffer bb = ByteBuffer.wrap(b);
			while (bb.hasRemaining())
			{
				int written = fc.write(bb, offset);
				if (0 == written)
				{
					throw new IOException("Nothing got written .. should not occur");
				}
				offset += written;
			}
		}
	}

	private int addPage(DataPageV2 abstractDataPage)
	{
		DataPageV2 dataPage = abstractDataPage;
		DataPageHeaderV2 dataPageHeader = new DataPageHeaderV2();
		dataPageHeader.setNum_values(dataPage.getValueCount());
		dataPageHeader.setNum_nulls(dataPage.getNullCount());
		dataPageHeader.setNum_rows(dataPage.getRowCount());
		Encoding enc = convert(dataPage.getDataEncoding());
		encodingSet.add(enc);
		dataPageHeader.setEncoding(enc);

		dataPageHeader.setDefinition_levels_byte_length(Math.toIntExact(dataPage.getDefinitionLevels().size()));
		dataPageHeader.setRepetition_levels_byte_length(Math.toIntExact(dataPage.getRepetitionLevels().size()));

		PageHeader pageHeader = new PageHeader();

		pageHeader.setUncompressed_page_size(dataPage.getUncompressedSize());
		pageHeader.setCompressed_page_size(dataPage.getCompressedSize());

		pageHeader.setType(PageType.DATA_PAGE_V2);
		pageHeader.setData_page_header_v2(dataPageHeader);

		int ret = 0;
		ret += storeHeaderBytes(pageHeader);
		ret += dataPage.getUncompressedSize();
		addBytes(dataPage.getDefinitionLevels());
		addBytes(dataPage.getRepetitionLevels());
		addBytes(dataPage.getData());
		return ret;
	}

	private int addBytes(BytesInput input)
	{
		try
		{
			if (input.size() > 0)
			{
				byte[] written = input.toByteArray();
				headersAndPages.add(written);
				return written.length;
			}
			return 0;
		}
		catch (IOException ex)
		{
			throw new UncheckedIOException(ex);
		}
	}

	private int storeHeaderBytes(PageHeader pageHeader)
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try
		{
			Util.writePageHeader(pageHeader, baos);
		}
		catch (IOException ex)
		{
			// should never happen with byte array output stream.
			throw new UncheckedIOException(ex);
		}
		byte[] written = baos.toByteArray();
		headersAndPages.add(written);
		return written.length;
	}

	private static Encoding convert(org.apache.parquet.column.Encoding encoding)
	{
		return Encoding.valueOf(encoding.name());
	}

	public ColumnDescriptor getColumnDescriptor()
	{
		return columnDescriptor;
	}

	public Set<Encoding> getEncodingSet()
	{
		return Collections.unmodifiableSet(encodingSet);
	}
}
