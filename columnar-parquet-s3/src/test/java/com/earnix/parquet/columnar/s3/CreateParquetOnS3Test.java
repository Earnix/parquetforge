package com.earnix.parquet.columnar.s3;

import com.earnix.parquet.columnar.s3.buffering.S3KeyUploader;
import com.earnix.parquet.columnar.reader.ParquetColumnarFileReader;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class CreateParquetOnS3Test
{
	@Test
	public void simple() throws Exception
	{
		try (S3MockService service = new S3MockService())
		{
			S3Client s3Client = service.getS3Client();
			String key = "test.parquet";
			S3KeyUploader uploader = new S3KeyUploader(s3Client, service.testBucket(), key);
			String colName = "testDouble";
			MessageType messageType = new MessageType("root",
					new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, colName));
			ParquetProperties properties = ParquetProperties.builder().build();
			try (ParquetColumnarWriter writer = new ParquetS3ObjectWriterImpl(messageType, CompressionCodec.ZSTD,
					properties, uploader, 2))
			{
				writer.writeRowGroup(1, rowGroupWriter -> rowGroupWriter.writeValues(
						chunkWriter -> chunkWriter.writeColumn(colName, new double[] { 1 })));
				writer.finishAndWriteFooterMetadata();
			}

			// download the entire file and ensure that it can be read
			Path tmpFile = Files.createTempFile("potato", ".parquet");
			try (InputStream resp = s3Client.getObject(builder -> builder.bucket(service.testBucket()).key(key)))
			{
				Files.copy(resp, tmpFile, StandardCopyOption.REPLACE_EXISTING);
			}

			ParquetColumnarFileReader reader = new ParquetColumnarFileReader(tmpFile);
			Assert.assertEquals(colName, reader.readMetaData().getSchema().get(1).getName());
		}
	}
}
