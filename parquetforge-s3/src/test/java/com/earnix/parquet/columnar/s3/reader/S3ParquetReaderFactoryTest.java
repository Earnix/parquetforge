package com.earnix.parquet.columnar.s3.reader;

import com.earnix.parquet.columnar.reader.IndexedParquetColumnarReader;
import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkValuesReaderFactory;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.s3.S3MockService;
import com.earnix.parquet.columnar.s3.downloader.S3KeyDownloader;
import com.earnix.parquet.columnar.s3.reader.S3ReaderTestUtils.UploadedParquet;
import org.apache.parquet.column.ColumnDescriptor;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class S3ParquetReaderFactoryTest
{
	@Test
	public void createReaderFromS3ClientReadsValues() throws Exception
	{
		try (S3MockService service = new S3MockService())
		{
			S3Client s3Client = service.getS3Client();
			String key = "factoryTest";
			UploadedParquet uploaded = S3ReaderTestUtils.uploadDoubleColumnParquet(s3Client, service.testBucket(), key,
					Arrays.asList(new double[] { 1.0, 2.0 }, new double[] { 3.0 }));

			S3KeyDownloader s3KeyDownloader = new S3KeyDownloader(s3Client, service.testBucket(), key);
			IndexedParquetColumnarReader reader = S3ParquetReaderFactory.createIndexedColumnarS3Reader(s3KeyDownloader);

			Assert.assertEquals(uploaded.messageType, reader.getMessageType());
			Assert.assertEquals(uploaded.numRowGroups(), reader.getNumRowGroups());

			Assert.assertEquals(Arrays.asList(1.0, 2.0), readValues(reader, uploaded.descriptor, 0));
			Assert.assertEquals(Arrays.asList(3.0), readValues(reader, uploaded.descriptor, 1));
			Assert.assertEquals(uploaded.descriptor, reader.getDescriptorByPath(S3ReaderTestUtils.COLUMN_NAME));
		}
	}

	@Test
	public void createReaderFromDownloaderUsesProvidedDownloader() throws Exception
	{
		try (S3MockService service = new S3MockService())
		{
			S3Client s3Client = service.getS3Client();
			String key = "SoemeOtherTest";
			UploadedParquet uploaded = S3ReaderTestUtils.uploadDoubleColumnParquet(s3Client, service.testBucket(), key,
					Arrays.asList(new double[] { 4.0 }, new double[] { 5.0, 6.0 }));

			S3KeyDownloader downloader = new S3KeyDownloader(s3Client, service.testBucket(), key);
			IndexedParquetColumnarReader reader = S3ParquetReaderFactory.createIndexedColumnarS3Reader(downloader);

			Assert.assertEquals(uploaded.totalRows(), reader.getTotalNumRows());
			Assert.assertEquals(Arrays.asList(4.0), readValues(reader, uploaded.descriptor, 0));
			Assert.assertEquals(Arrays.asList(5.0, 6.0), readValues(reader, uploaded.descriptor, 1));
		}
	}

	private static List<Double> readValues(IndexedParquetColumnarReader reader, ColumnDescriptor descriptor, int rowGroup)
			throws IOException
	{
		InMemChunk chunk = reader.readInMem(rowGroup, descriptor);
		ChunkValuesReader valuesReader = ChunkValuesReaderFactory.createChunkReader(chunk);
		List<Double> values = new ArrayList<>();
		do
		{
			values.add(valuesReader.getDouble());
		}
		while (valuesReader.next());
		return values;
	}
}

