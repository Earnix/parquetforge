package com.earnix.parquet.columnar.s3.reader;

import com.earnix.parquet.columnar.s3.S3MockService;
import com.earnix.parquet.columnar.s3.buffering.S3KeyUploader;
import com.earnix.parquet.columnar.s3.downloader.S3KeyDownloader;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class S3RangeInputStreamSupplierTest
{
	/**
	 * Test range downloader works as intended
	 */
	@Test
	public void getReturnsRequestedRange() throws Exception
	{
		byte[] payload = "0123456789abcde01234567".getBytes(StandardCharsets.UTF_8);

		try (S3MockService service = new S3MockService())
		{
			S3Client s3Client = service.getS3Client();
			String key = "test";
			s3Client.putObject(builder -> builder.bucket(service.testBucket()).key(key),
					RequestBody.fromBytes(payload));

			S3KeyDownloader downloader = new S3KeyDownloader(s3Client, service.testBucket(), key);

			try (InputStream is = new S3RangeInputStreamSupplier(downloader, 5, 10).get())
			{
				byte[] data = IOUtils.toByteArray(is);
				Assert.assertArrayEquals("56789abcde".getBytes(StandardCharsets.UTF_8), data);
			}
		}
	}
}

