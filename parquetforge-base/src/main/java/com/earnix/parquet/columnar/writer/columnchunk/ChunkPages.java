package com.earnix.parquet.columnar.writer.columnchunk;

import com.earnix.parquet.columnar.utils.ParquetEnumUtils;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * All pages needed for a particular chunk. This class is Immutable and Thread Safe. It is safe to write the outputted
 * column pages to multiple files and/or streams at the same time
 */
public class ChunkPages
{
	private final Set<Encoding> encodingSet = EnumSet.noneOf(Encoding.class);
	private final List<byte[]> headersAndPages;
	private final long numValues;
	private final long uncompressedBytes;
	private final long compressedBytes;
	private final CompressionCodec compressionCodec;

	public ChunkPages(DictionaryPage dictionaryPage, List<? extends DataPage> dataPages,
			CompressionCodec compressionCodec)
	{
		this.compressionCodec = compressionCodec;
		int numPages = dictionaryPage == null ? dataPages.size() : dataPages.size() + 1;
		this.headersAndPages = new ArrayList<>(2 * numPages);
		long uncompressedBytes = 0;
		if (dictionaryPage != null)
		{
			DictionaryPageHeader dictionaryPageHeader = new DictionaryPageHeader();
			Encoding enc = ParquetEnumUtils.convert(dictionaryPage.getEncoding());
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

	/**
	 * @return The total number of bytes that this chunk page will take when persisted
	 */
	public long totalBytesForStorage()
	{
		return compressedBytes;
	}

	/**
	 * @return The number of bytes this chunk will take when the pages are uncompressed.
	 */
	public long getUncompressedBytes()
	{
		return uncompressedBytes;
	}

	/**
	 * @return The number of values stored in this page
	 */
	public long getNumValues()
	{
		return numValues;
	}

	/**
	 * @return the bytes of this page as an {@link InputStream}
	 */
	public InputStream toInputStream()
	{
		return new SequenceInputStream(Collections.enumeration(
				headersAndPages.stream().map(ByteArrayInputStream::new).collect(Collectors.toList())));
	}

	/**
	 * Write the bytes of this column page to an output stream
	 *
	 * @param os the output stream to write the bytes to
	 * @throws IOException on failure to write to the OutputStream
	 */
	public void writeToOutputStream(OutputStream os) throws IOException
	{
		for (byte[] toWrite : headersAndPages)
			os.write(toWrite);
	}

	/**
	 * Write the bytes of this page to a file
	 *
	 * @param fc             the file channel of the file
	 * @param startingOffset the starting offset to write to in the file
	 * @throws IOException on failure to write to the file
	 */
	public void writeToFile(FileChannel fc, long startingOffset) throws IOException
	{
		long offset = startingOffset;
		for (byte[] b : this.headersAndPages)
		{
			ByteBuffer bb = ByteBuffer.wrap(b);
			offset += ChunkWritingUtils.writeByteBufferToChannelFully(fc, bb, offset);
		}
	}

	private int addPage(DataPageV2 dataPage)
	{
		DataPageHeaderV2 dataPageHeader = new DataPageHeaderV2();
		dataPageHeader.setNum_values(dataPage.getValueCount());
		dataPageHeader.setNum_nulls(dataPage.getNullCount());
		dataPageHeader.setNum_rows(dataPage.getRowCount());
		Encoding enc = ParquetEnumUtils.convert(dataPage.getDataEncoding());
		encodingSet.add(enc);
		dataPageHeader.setEncoding(enc);

		dataPageHeader.setDefinition_levels_byte_length(Math.toIntExact(dataPage.getDefinitionLevels().size()));
		dataPageHeader.setRepetition_levels_byte_length(Math.toIntExact(dataPage.getRepetitionLevels().size()));
		dataPageHeader.setIs_compressed(dataPage.isCompressed());

		PageHeader pageHeader = new PageHeader();

		pageHeader.setUncompressed_page_size(dataPage.getUncompressedSize());
		pageHeader.setCompressed_page_size(dataPage.getCompressedSize());

		pageHeader.setType(PageType.DATA_PAGE_V2);
		pageHeader.setData_page_header_v2(dataPageHeader);

		final int headerSizeInBytes = storeHeaderBytes(pageHeader);

		dataPage.getUncompressedSize();
		addBytes(dataPage.getRepetitionLevels());
		addBytes(dataPage.getDefinitionLevels());

		addBytes(dataPage.getData());
		return headerSizeInBytes + dataPage.getUncompressedSize();
	}

	private void addBytes(BytesInput input)
	{
		try
		{
			if (input.size() > 0)
			{
				byte[] written = input.toByteArray();
				headersAndPages.add(written);
			}
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

	/**
	 * @return the set of {@link Encoding} used by these column chunk pages (must be populated in metadata of parquet
	 * 		file)
	 */
	public Set<Encoding> getEncodingSet()
	{
		return Collections.unmodifiableSet(encodingSet);
	}

	/**
	 * @return the compression codec used for these chunk pages
	 */
	public CompressionCodec getCompressionCodec()
	{
		return compressionCodec;
	}
}
