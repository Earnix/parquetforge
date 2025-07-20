package com.earnix.parquet.columnar.s3.downloader;

import com.earnix.parquet.columnar.s3.S3MockService;
import com.earnix.parquet.columnar.s3.buffering.S3KeyUploader;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class S3KeyDownloaderTest
{

	@Test
	public void testDownload() throws IOException
	{
		byte[] dataPart1 = new byte[1024 * 1024];
		Random random = new Random(12345);

		// load data with random bytes
		random.nextBytes(dataPart1);

		try (S3MockService service = new S3MockService())
		{
			S3Client s3Client = service.getS3Client();
			String key = "testfile.bin";
			S3KeyUploader uploader = new S3KeyUploader(s3Client, service.testBucket(), key);

			uploader.uploadPart(1, dataPart1.length, () -> new ByteArrayInputStream(dataPart1));
			uploader.finish();

			S3KeyDownloader s3KeyDownloader = new S3KeyDownloader(s3Client, service.testBucket(), key);
			Assert.assertEquals(dataPart1.length, s3KeyDownloader.getObjectSize());
			byte[] lastBytes = s3KeyDownloader.getByteRange(dataPart1.length - 8, dataPart1.length);
			Assert.assertEquals(8, lastBytes.length);
			byte[] expecteds = Arrays.copyOfRange(dataPart1, dataPart1.length - 8, dataPart1.length);
			Assert.assertArrayEquals(expecteds, lastBytes);

			byte[] lastBytes2 = s3KeyDownloader.getLastBytes(8);
			Assert.assertEquals(8, lastBytes2.length);
			Assert.assertArrayEquals(expecteds, lastBytes2);

			byte[] lastBytes3 = new byte[expecteds.length];
			s3KeyDownloader.downloadRange(dataPart1.length - 8, dataPart1.length, is -> {
				IOUtils.readFully(is, lastBytes3);
				Assert.assertEquals(-1, is.read());
			});
			Assert.assertEquals(8, lastBytes2.length);
			Assert.assertArrayEquals(expecteds, lastBytes2);
		}
	}
}
