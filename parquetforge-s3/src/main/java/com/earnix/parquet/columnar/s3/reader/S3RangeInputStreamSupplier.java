package com.earnix.parquet.columnar.s3.reader;

import com.earnix.parquet.columnar.s3.downloader.S3KeyDownloader;
import org.apache.commons.io.function.IOSupplier;

import java.io.IOException;
import java.io.InputStream;

/**
 * Supplies input streams that stream a bounded range from an S3 object.
 */
public class S3RangeInputStreamSupplier implements IOSupplier<InputStream>
{
	private final S3KeyDownloader s3KeyDownloader;
	private final long startOffset;
	private final long numBytesToRead;

	public S3RangeInputStreamSupplier(S3KeyDownloader s3KeyDownloader, long startOffset, long numBytesToRead)
	{
		this.s3KeyDownloader = s3KeyDownloader;
		this.startOffset = startOffset;
		this.numBytesToRead = numBytesToRead;
	}

	@Override
	public InputStream get()
	{
		return s3KeyDownloader.openRange(startOffset, numBytesToRead);
	}

	public long getStartOffset()
	{
		return startOffset;
	}

	public long getNumBytesToRead()
	{
		return numBytesToRead;
	}
}

