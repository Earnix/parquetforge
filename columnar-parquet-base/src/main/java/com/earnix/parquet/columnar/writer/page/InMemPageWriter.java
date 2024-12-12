package com.earnix.parquet.columnar.writer.page;

import static org.apache.parquet.bytes.BytesInput.copy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.earnix.parquet.columnar.writer.compressors.Compressor;
import com.earnix.parquet.columnar.writer.compressors.CompressorSnappyImpl;
import com.earnix.parquet.columnar.writer.compressors.CompressorZstdImpl;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.SizeStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.io.ParquetEncodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapted from MemPageWriter in tests of parquet-column
 */
public class InMemPageWriter implements PageWriter
{
	private static final Logger LOG = LoggerFactory.getLogger(InMemPageWriter.class);

	private final List<DataPage> pages = new ArrayList<>();
	private DictionaryPage dictionaryPage;
	private long memSize = 0;
	private long totalValueCount = 0;
	private final Compressor compressor;
	private final CompressionCodec compressionCodec;

	public InMemPageWriter()
	{
		this(CompressionCodec.UNCOMPRESSED);
	}

	public InMemPageWriter(CompressionCodec compressionCodec)
	{
		this.compressionCodec = compressionCodec;
		switch (compressionCodec)
		{
			case SNAPPY:
				this.compressor = new CompressorSnappyImpl();
				break;
			case ZSTD:
				this.compressor = new CompressorZstdImpl();
				break;
			case UNCOMPRESSED:
				this.compressor = null;
				break;
			default:
				throw new IllegalArgumentException("Unsupported compression: " + compressionCodec);
		}
	}

	@Override
	public void writePage(BytesInput bytesInput, int valueCount, Statistics statistics, Encoding rlEncoding,
			Encoding dlEncoding, Encoding valuesEncoding) throws IOException
	{
		if (valueCount == 0)
		{
			throw new ParquetEncodingException("illegal page of 0 values");
		}

		// TODO: should this measure compressed size or uncompressed?? what is it for??
		memSize += bytesInput.size();

		byte[] toCompress = bytesInput.toByteArray();
		byte[] compressed = new byte[compressor.maxCompressedLength(toCompress.length)];
		int compressedLen = compressor.compress(toCompress, compressed);
		BytesInput compressedInput = buildCompressedInput(compressedLen, compressed);
		pages.add(new DataPageV1(compressedInput, valueCount, toCompress.length, statistics, rlEncoding, dlEncoding,
				valuesEncoding));
		totalValueCount += valueCount;
		LOG.debug("page written for {} bytes and {} records", bytesInput.size(), valueCount);
	}

	private static BytesInput buildCompressedInput(int compressedLen, byte[] compressed)
	{
		if (compressedLen < compressed.length / 2)
			compressed = Arrays.copyOf(compressed, compressedLen);

		BytesInput compressedInput = BytesInput.from(compressed, 0, compressedLen);
		return compressedInput;
	}

	@Override
	public void writePage(BytesInput bytesInput, int valueCount, int rowCount, Statistics<?> statistics,
			Encoding rlEncoding, Encoding dlEncoding, Encoding valuesEncoding) throws IOException
	{
		writePage(bytesInput, valueCount, statistics, rlEncoding, dlEncoding, valuesEncoding);
	}

	@Override
	public void writePage(BytesInput bytesInput, int valueCount, int rowCount, Statistics<?> statistics,
			SizeStatistics sizeStatistics, Encoding rlEncoding, Encoding dlEncoding, Encoding valuesEncoding)
			throws IOException
	{
		writePage(bytesInput, valueCount, statistics, rlEncoding, dlEncoding, valuesEncoding);
	}

	@Override
	public void writePageV2(int rowCount, int nullCount, int valueCount, BytesInput repetitionLevels,
			BytesInput definitionLevels, Encoding dataEncoding, BytesInput data, Statistics<?> statistics)
			throws IOException
	{
		if (valueCount == 0)
		{
			throw new ParquetEncodingException("illegal page of 0 values");
		}
		long size = repetitionLevels.size() + definitionLevels.size() + data.size();
		// TODO: should this measure compressed size or uncompressed?? what is it for??
		memSize += size;

		BytesInput dataInput = null;
		if (compressor != null)
		{
			byte[] toCompress = data.toByteArray();
			byte[] compressed = new byte[compressor.maxCompressedLength(toCompress.length)];
			int compressedLen = compressor.compress(toCompress, compressed);

			// if the compressed length is greater or equal to than the uncompressed size, don't compress!
			if (compressedLen < toCompress.length)
			{
				dataInput = buildCompressedInput(compressedLen, compressed);
				int uncompressedLen = Math
						.toIntExact(repetitionLevels.size() + definitionLevels.size() + toCompress.length);
				pages.add(DataPageV2.compressed(rowCount, nullCount, valueCount, copy(repetitionLevels),
						copy(definitionLevels), dataEncoding, dataInput, uncompressedLen, statistics));
			}
		}

		if (dataInput == null)
		{
			pages.add(DataPageV2.uncompressed(rowCount, nullCount, valueCount, copy(repetitionLevels),
					copy(definitionLevels), dataEncoding, copy(data), statistics));
		}

		totalValueCount += valueCount;
		LOG.debug("page written for {} bytes and {} records", size, valueCount);
	}

	@Override
	public void writePageV2(int rowCount, int nullCount, int valueCount, BytesInput repetitionLevels,
			BytesInput definitionLevels, Encoding dataEncoding, BytesInput data, Statistics<?> statistics,
			SizeStatistics sizeStatistics) throws IOException
	{
		writePageV2(rowCount, nullCount, valueCount, repetitionLevels, definitionLevels, dataEncoding, data,
				statistics);
	}

	@Override
	public long getMemSize()
	{
		return memSize;
	}

	public List<DataPage> getPages()
	{
		return pages;
	}

	public DictionaryPage getDictionaryPage()
	{
		return dictionaryPage;
	}

	public long getTotalValueCount()
	{
		return totalValueCount;
	}

	@Override
	public long allocatedSize()
	{
		// this store keeps only the bytes written
		return memSize;
	}

	@Override
	public void writeDictionaryPage(DictionaryPage dictionaryPage) throws IOException
	{
		if (this.dictionaryPage != null)
		{
			throw new ParquetEncodingException("Only one dictionary page per block");
		}
		this.memSize += dictionaryPage.getBytes().size();
		if (compressor != null)
		{
			if (dictionaryPage.getCompressedSize() != dictionaryPage.getUncompressedSize())
				throw new IllegalStateException("Dictionary should not be compressed at this point..");

			byte[] toCompress = dictionaryPage.getBytes().toByteArray();
			byte[] compressed = new byte[compressor.maxCompressedLength(toCompress.length)];
			int compressedLen = compressor.compress(toCompress, compressed);
			this.dictionaryPage = new DictionaryPage(buildCompressedInput(compressedLen, compressed),
					dictionaryPage.getUncompressedSize(), dictionaryPage.getDictionarySize(),
					dictionaryPage.getEncoding());
		}
		else
		{
			this.dictionaryPage = dictionaryPage.copy();
		}
		LOG.debug("dictionary page written for {} bytes and {} records", dictionaryPage.getBytes().size(),
				dictionaryPage.getDictionarySize());
	}

	@Override
	public String memUsageString(String prefix)
	{
		return String.format("%s %,d bytes", prefix, memSize);
	}

}
