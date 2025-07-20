package com.earnix.parquet.columnar.s3.buffering;

import com.earnix.parquet.columnar.s3.S3MockService;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class S3KeyUploaderTest
{
	@Test
	public void testUploadPart() throws IOException
	{
		byte[] dataPart1 = new byte[6 * 1024 * 1024];
		byte[] dataPart2 = new byte[2 * 1024 * 1024];
		Random random = new Random(12345);

		// load data with random bytes
		random.nextBytes(dataPart1);
		random.nextBytes(dataPart2);

		try (S3MockService service = new S3MockService())
		{
			S3Client s3Client = service.getS3Client();
			String key = "testfile.bin";
			S3KeyUploader uploader = new S3KeyUploader(s3Client, service.testBucket(), key);

			uploader.uploadPart(1, dataPart1.length, () -> new ByteArrayInputStream(dataPart1));
			uploader.uploadPart(3, dataPart2.length, () -> new ByteArrayInputStream(dataPart2));
			uploader.finish();


			byte[] fetchedBytes = s3Client.getObjectAsBytes(builder -> builder.bucket(service.testBucket()).key(key))
					.asByteArrayUnsafe();

			byte[] expected = Arrays.copyOf(dataPart1, dataPart1.length + dataPart2.length);
			System.arraycopy(dataPart2, 0, expected, dataPart1.length, dataPart2.length);
			Assert.assertArrayEquals(expected, fetchedBytes);
		}
	}
}
