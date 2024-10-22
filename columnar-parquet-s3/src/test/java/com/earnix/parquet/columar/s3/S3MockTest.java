package com.earnix.parquet.columar.s3;

import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;

import java.util.Collections;

/**
 * A simple test to ensure that the s3 mock works
 */
public class S3MockTest
{
	@Test
	public void testSimpleAS3UploadListDownload()
	{
		try (S3MockService service = new S3MockService())
		{
			S3Client s3Client = service.getS3Client();
			ListObjectsResponse resp = s3Client.listObjects(req -> req.bucket(service.testBucket()).prefix(""));
			Assert.assertEquals(Collections.emptyList(), resp.contents());
			String testObj = "test.bin";
			s3Client.putObject(req -> req.bucket(service.testBucket()).key(testObj), RequestBody.empty());

			resp = s3Client.listObjects(req -> req.bucket(service.testBucket()).prefix(""));
			Assert.assertEquals(1, resp.contents().size());
		}
	}
}
