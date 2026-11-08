package com.earnix.parquet.columnar.utils;

import com.earnix.parquet.columnar.reader.IndexedParquetColumnarReader;
import com.earnix.parquet.columnar.reader.IndexedParquetColumnarReaderImpl;
import com.earnix.parquet.columnar.reader.ParquetMetadataReaderUtils;
import com.earnix.parquet.columnar.reader.ParquetReaderInputStreamSupplier;
import org.apache.commons.io.function.Uncheck;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Optional;

import static java.lang.Math.toIntExact;

/**
 * Utility to check whether byte array is plausibly valid parquet data
 */
public class ParquetInMemReaderUtils
{
	private static final Logger LOG = LoggerFactory.getLogger(ParquetInMemReaderUtils.class);

	// min length for parquet is at least 12 bytes - magic twice, metadata len, and 1 byte for metadata.
	// Probably needs to be longer in practice but this is a safe minimum.
	private static final int SANITY_MIN_LENGTH = 13;

	public static Optional<IndexedParquetColumnarReader> readIntoIndexParquetColumnarReader(byte[] data)
	{
		var ret = readMetadataIfValidParquetFile(data).map(md -> new ParquetReaderInputStreamSupplier()
		{
			@Override
			public FileMetaData readMetaData()
			{
				return md;
			}

			@Override
			public InputStream createInputStream(long startOffset, long numBytesToRead) throws IOException
			{
				return new ByteArrayInputStream(data, toIntExact(startOffset), toIntExact(numBytesToRead));
			}
		}).map(isSupplier -> Uncheck.get(
				() -> (IndexedParquetColumnarReader) new IndexedParquetColumnarReaderImpl(isSupplier)));

		return ret;
	}

	private static Optional<FileMetaData> readMetadataIfValidParquetFile(byte[] data)
	{
		if (data.length < SANITY_MIN_LENGTH)
		{
			return Optional.empty();
		}
		// check magic at beginning and end.
		ByteBuffer buffer = ByteBuffer.wrap(data);

		// begin magic
		if (!ParquetMagicUtils.expectMagic(buffer))
		{
			return Optional.empty();
		}

		buffer.position(data.length - ParquetMagicUtils.PARQUET_MAGIC.length());

		// end magic
		if (!ParquetMagicUtils.expectMagic(buffer))
		{
			return Optional.empty();
		}

		buffer.position(data.length - ParquetMetadataReaderUtils.FOOTER_LEN_AND_MAGIC);
		int footerMetadataLen = ParquetMetadataReaderUtils.readFooterLenAndMagic(buffer);

		int startPos = data.length - footerMetadataLen - ParquetMetadataReaderUtils.FOOTER_LEN_AND_MAGIC;
		try (InputStream in = new ByteArrayInputStream(data, startPos, footerMetadataLen))
		{
			var metadata = Util.readFileMetaData(in);
			return Optional.ofNullable(metadata);
		}
		catch (Exception e)
		{
			LOG.debug("Magic found but failed to decode metadata from byte[]", e);
		}
		return Optional.empty();
	}
}
