package com.earnix.parquet.columnar.s3;

import com.adobe.testing.s3mock.S3MockApplication;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import java.util.List;
import java.util.stream.Collectors;

public class S3MockService implements AutoCloseable
{
	private final S3MockApplication s3MockApplication = S3MockUtils.createAndStartNewS3Mock();
	private final S3Client s3Client = S3MockUtils.getS3Client(s3MockApplication);

	public S3MockService()
	{
		resetTestBucket(false);
	}

	public S3Client getS3Client()
	{
		return s3Client;
	}

	public String testBucket()
	{
		return "testbucket";
	}

	public void resetTestBucket()
	{
		resetTestBucket(true);
	}

	private void resetTestBucket(boolean deleteBucket)
	{
		if (deleteBucket)
		{
			ListObjectsV2Response resp = null;
			do
			{
				String continuationToken = resp == null ? null : resp.nextContinuationToken();
				resp = s3Client.listObjectsV2(
						builder -> builder.bucket(testBucket()).prefix("").continuationToken(continuationToken));
				if (resp.hasContents())
				{
					List<ObjectIdentifier> toDelete = resp.contents().stream()
							.map(obj -> ObjectIdentifier.builder().key(obj.key()).build()).collect(Collectors.toList());
					s3Client.deleteObjects(DeleteObjectsRequest.builder().bucket(testBucket())
							.delete(Delete.builder().objects(toDelete).build()).build());
				}
			}
			while (resp.isTruncated());

			s3Client.deleteBucket(builder -> builder.bucket(testBucket()));
		}
		s3Client.createBucket(builder -> builder.bucket(testBucket()));
	}

	@Override
	public void close()
	{
		try
		{
			s3Client.close();
		}
		finally
		{
			try
			{
				s3MockApplication.stop();
			}
			catch (Exception ex)
			{
				ex.printStackTrace();
			}
		}
	}
}
