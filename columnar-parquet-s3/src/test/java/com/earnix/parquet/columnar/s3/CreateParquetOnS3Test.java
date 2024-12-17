package com.earnix.parquet.columnar.s3;

import com.earnix.parquet.columnar.reader.IndexedParquetColumnarFileReader;
import com.earnix.parquet.columnar.reader.ParquetColumnarFileReader;
import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkValuesReaderFactory;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.s3.assembler.ParquetFileChunkSupplier;
import com.earnix.parquet.columnar.s3.assembler.ParquetRowGroupSupplier;
import com.earnix.parquet.columnar.s3.assembler.S3ParquetAssembleAndUpload;
import com.earnix.parquet.columnar.s3.buffering.S3KeyUploader;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import com.earnix.parquet.columnar.writer.ParquetFileColumnarWriterFactory;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;

public class CreateParquetOnS3Test
{
	@Test
	public void tinyParquetFileWithRowGroupBuffering() throws Exception
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

	@Test
	public void tinyParquetFileAssembled() throws Exception
	{
		Path tmpFile = Files.createTempFile("test", ".parquet");
		Path sourceFilePath = Paths.get("test.parquet");
		String colName = "testDouble";
		PrimitiveType[] primitiveTypes = new PrimitiveType[] {
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, colName) };
		try (ParquetColumnarWriter writer = ParquetFileColumnarWriterFactory.createWriter(sourceFilePath,
				Arrays.asList(primitiveTypes)))
		{
			writer.writeRowGroup(1, rowGroupWriter -> rowGroupWriter.writeValues(
					chunkWriter -> chunkWriter.writeColumn(colName, new double[] { 1 })));
			writer.writeRowGroup(1, rowGroupWriter -> rowGroupWriter.writeValues(
					chunkWriter -> chunkWriter.writeColumn(colName, new double[] { 2 })));
			writer.finishAndWriteFooterMetadata();
		}

		// assemble and upload
		IndexedParquetColumnarFileReader localReader = new IndexedParquetColumnarFileReader(sourceFilePath);

		String keyOnS3 = "test.parquet";

		Path tmpFile2 = Files.createTempFile("potato", ".parquet");
		MessageType messageType = new MessageType("root", primitiveTypes);

		try (S3MockService service = new S3MockService())
		{
			S3Client s3Client = service.getS3Client();
			S3KeyUploader uploader = new S3KeyUploader(s3Client, service.testBucket(), keyOnS3);
			S3ParquetAssembleAndUpload assembler = new S3ParquetAssembleAndUpload(messageType, 2, 1);

			// read the blocks backward
			ColumnDescriptor descriptor = localReader.getDescriptor(0);
			ParquetRowGroupSupplier parquetRowGroupSupplier = ParquetRowGroupSupplier.builder()
					.addChunkSupplier(new ParquetFileChunkSupplier(localReader, descriptor, 1)).build();
			ParquetRowGroupSupplier parquetRowGroupSupplier2 = ParquetRowGroupSupplier.builder()
					.addChunkSupplier(new ParquetFileChunkSupplier(localReader, descriptor, 0)).build();

			assembler.assembleAndUpload(uploader, Arrays.asList(parquetRowGroupSupplier, parquetRowGroupSupplier2));


			downloadAndValidate(s3Client, service, keyOnS3, tmpFile, colName, descriptor);

			Files.deleteIfExists(tmpFile2);
		}
	}

	private static void downloadAndValidate(S3Client s3Client, S3MockService service, String keyOnS3, Path tmpFile,
			String colName, ColumnDescriptor descriptor) throws IOException
	{
		// download and test
		try (InputStream resp = s3Client.getObject(builder -> builder.bucket(service.testBucket()).key(keyOnS3)))
		{
			Files.copy(resp, tmpFile, StandardCopyOption.REPLACE_EXISTING);
		}

		IndexedParquetColumnarFileReader reader = new IndexedParquetColumnarFileReader(tmpFile);
		Assert.assertEquals(colName, reader.readMetaData().getSchema().get(1).getName());

		// make sure that we're pointing at different places.
		Assert.assertNotEquals(reader.getColumnChunk(0, descriptor).getMeta_data().getData_page_offset(),
				reader.getColumnChunk(1, descriptor).getMeta_data().getData_page_offset());
		for (RowGroup rowGroup : reader.readMetaData().getRow_groups())
		{
			Assert.assertEquals(1, rowGroup.getNum_rows());
		}

		assertValue(reader, descriptor, 0, 2.0d);
		assertValue(reader, descriptor, 1, 1.0d);
	}

	private static void assertValue(IndexedParquetColumnarFileReader reader, ColumnDescriptor descriptor, int rowGrp,
			double expected) throws IOException
	{
		InMemChunk chunk = reader.readInMem(rowGrp, descriptor);
		Assert.assertEquals(1, chunk.getTotalValues());
		ChunkValuesReader valReader = ChunkValuesReaderFactory.createChunkReader(chunk);
		Assert.assertFalse(valReader.isNull());
		Assert.assertEquals(expected, valReader.getDouble(), 0.0d);
		Assert.assertFalse(valReader.next());
	}
}
