package com.earnix.parquet.columnar.s3.reader;

import com.earnix.parquet.columnar.s3.downloader.S3KeyDownloader;
import com.earnix.parquet.columnar.utils.ParquetMagicUtils;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Reads parquet metadata for objects stored on S3.
 */
public final class S3ParquetMetadataReaderUtils
{
	private S3ParquetMetadataReaderUtils()
	{
	}

	public static FileMetaData readFileMetadata(S3KeyDownloader s3KeyDownloader) throws IOException
	{
		int lenAndMagicFooter = Integer.BYTES + ParquetMagicUtils.PARQUET_MAGIC.length();
		byte[] lastBytes = s3KeyDownloader.getLastBytes(lenAndMagicFooter);
		ByteBuffer buf = ByteBuffer.wrap(lastBytes).order(ByteOrder.LITTLE_ENDIAN);
		int footerMetadataLen = buf.getInt();
		ParquetMagicUtils.expectMagic(buf);

		byte[] footerMetadataBytes = s3KeyDownloader.getLastBytes(footerMetadataLen + lenAndMagicFooter);
		return Util.readFileMetaData(new ByteArrayInputStream(footerMetadataBytes));
	}
}

