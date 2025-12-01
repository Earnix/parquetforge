package com.earnix.parquet.columnar.s3.reader;

import com.earnix.parquet.columnar.reader.IndexedParquetColumnarReader;
import com.earnix.parquet.columnar.reader.IndexedParquetColumnarReaderImpl;
import com.earnix.parquet.columnar.reader.ParquetReaderInputStreamSupplier;
import com.earnix.parquet.columnar.s3.downloader.S3KeyDownloader;
import org.apache.parquet.format.FileMetaData;

import java.io.IOException;
import java.io.InputStream;

/**
 * Factory to build {@link IndexedParquetColumnarReader} instances that read from S3.
 * 
 * Note: S3 indexed parquet reader may yield poor performance. It is designed for directory buckets.
 */
public final class S3ParquetReaderFactory
{
	

	public static IndexedParquetColumnarReader createIndexedColumnarS3Reader(S3KeyDownloader s3KeyDownloader)
			throws IOException
	{
		return new IndexedParquetColumnarReaderImpl(new ParquetReaderInputStreamSupplier()
		{
			@Override
			public FileMetaData readMetaData() throws IOException
			{
				return S3ParquetMetadataReaderUtils.readFileMetadata(s3KeyDownloader);
			}

			@Override
			public InputStream createInputStream(long startOffset, long numBytesToRead) throws IOException
			{
				return new S3RangeInputStreamSupplier(s3KeyDownloader, startOffset, numBytesToRead).get();
			}
		});
	}
}

