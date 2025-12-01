package com.earnix.parquet.columnar.s3.reader;

import com.earnix.parquet.columnar.s3.S3MockService;
import com.earnix.parquet.columnar.s3.buffering.S3KeyUploader;
import com.earnix.parquet.columnar.s3.downloader.S3KeyDownloader;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class S3RangeInputStreamSupplierTest
{
	@Test
	public void getReturnsRequestedRange() throws Exception
	{
		byte[] payload = "0123456789abcde01234567".getBytes(StandardCharsets.UTF_8);

		try (S3MockService service = new S3MockService())
		{
			S3Client s3Client = service.getS3Client();
			String key = "test";
			try (S3KeyUploader uploader = new S3KeyUploader(s3Client, service.testBucket(), key))
			{
				uploader.uploadPart(1, payload.length, () -> new ByteArrayInputStream(payload));
				uploader.finish();
			}

			S3KeyDownloader downloader = new S3KeyDownloader(s3Client, service.testBucket(), key);
			S3RangeInputStreamSupplier supplier = new S3RangeInputStreamSupplier(downloader, 5, 10);

			try (InputStream is = supplier.get())
			{
				byte[] data = IOUtils.toByteArray(is);
				Assert.assertArrayEquals("56789abcde".getBytes(StandardCharsets.UTF_8), data);
			}
		}
	}
}

