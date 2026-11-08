package com.earnix.parquet.columnar.reader;

import com.earnix.parquet.columnar.utils.ParquetMagicUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ParquetMetadataReaderUtils
{
	private static final Logger logger = LoggerFactory.getLogger(ParquetMetadataReaderUtils.class);

	private static final byte[] magicBytes = ParquetMagicUtils.PARQUET_MAGIC.getBytes(StandardCharsets.US_ASCII);
	// reading footer metadata means reading the integer length of the footer, and then the magic bytes.
	public static final int FOOTER_LEN_AND_MAGIC = Integer.BYTES + magicBytes.length;

	public static int readFooterLenAndMagic(ByteBuffer buf)
	{
		if (buf.remaining() != FOOTER_LEN_AND_MAGIC)
			throw new IllegalStateException();
		int numBytesInFooters = buf.getInt();
		logger.debug("Footers are {} bytes", numBytesInFooters);

		// validate magic at footer
		assertMagic(buf);
		return numBytesInFooters;
	}

	public static void assertMagic(ByteBuffer buf)
	{
		if (!ParquetMagicUtils.expectMagic(buf))
			throw new IllegalStateException("Parquet file did not contain expected magic");
	}
}
