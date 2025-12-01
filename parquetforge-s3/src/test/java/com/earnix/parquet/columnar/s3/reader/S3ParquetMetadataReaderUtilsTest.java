package com.earnix.parquet.columnar.s3.reader;

import com.earnix.parquet.columnar.s3.S3MockService;
import com.earnix.parquet.columnar.s3.downloader.S3KeyDownloader;
import com.earnix.parquet.columnar.s3.reader.S3ReaderTestUtils.UploadedParquet;
import org.apache.parquet.format.FileMetaData;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Arrays;

public class S3ParquetMetadataReaderUtilsTest
{
	@Test
	public void readFileMetadataReturnsFooterFromS3() throws Exception
	{
		try (S3MockService service = new S3MockService())
		{
			S3Client s3Client = service.getS3Client();
			String key = "metadataTest";
			UploadedParquet uploaded = S3ReaderTestUtils.uploadDoubleColumnParquet(s3Client, service.testBucket(), key,
					Arrays.asList(new double[] { 1.0, 2.0 }, new double[] { 3.0 }));

			FileMetaData metaData = S3ParquetMetadataReaderUtils
					.readFileMetadata(new S3KeyDownloader(s3Client, service.testBucket(), key));

			Assert.assertEquals(uploaded.totalRows(), metaData.getNum_rows());
			Assert.assertEquals(uploaded.numRowGroups(), metaData.getRow_groupsSize());
			Assert.assertEquals("root", metaData.getSchema().get(0).getName());
			Assert.assertEquals(S3ReaderTestUtils.COLUMN_NAME, metaData.getSchema().get(1).getName());
			Assert.assertEquals(uploaded.rowsInGroup(0), metaData.getRow_groups().get(0).getNum_rows());
			Assert.assertEquals(uploaded.rowsInGroup(1), metaData.getRow_groups().get(1).getNum_rows());
		}
	}
}

