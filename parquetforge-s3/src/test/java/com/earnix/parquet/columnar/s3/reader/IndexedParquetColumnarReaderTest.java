package com.earnix.parquet.columnar.s3.reader;

import com.earnix.parquet.columnar.file.reader.ParquetFileReaderFactory;
import com.earnix.parquet.columnar.reader.IndexedParquetColumnarReader;
import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkValuesReaderFactory;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.s3.S3MockService;
import com.earnix.parquet.columnar.s3.downloader.S3KeyDownloader;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import com.earnix.parquet.columnar.file.writer.ParquetFileColumnarWriterFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@RunWith(Parameterized.class)
public class IndexedParquetColumnarReaderTest
{
	private static final String COLUMN_NAME = "testDouble";

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> getTypes()
	{
		return Arrays.asList(new Object[][] { { ReaderType.FILE }, { ReaderType.S3 } });
	}

	private final ReaderType readerType;
	private Path parquetFile;
	private TestParquetData testData;
	private S3MockService s3MockService;
	private S3KeyDownloader downloader;
	private IndexedParquetColumnarReader reader; 

	public IndexedParquetColumnarReaderTest(ReaderType readerType)
	{
		this.readerType = readerType;
	}

	@Before
	public void setUp() throws Exception
	{
		parquetFile = Files.createTempFile("parameterized-reader", ".parquet");
		testData = TestParquetData.write(parquetFile, createDefaultRowGroups());
		if (readerType == ReaderType.S3)
		{
			s3MockService = new S3MockService();
			String s3Key = "parquet/" + UUID.randomUUID();
			S3Client s3Client = s3MockService.getS3Client();
			s3Client.putObject(builder -> builder.bucket(s3MockService.testBucket()).key(s3Key),
					RequestBody.fromFile(parquetFile));
			downloader = new S3KeyDownloader(s3Client, s3MockService.testBucket(), s3Key);
		}
		reader = createReader();
	}

	@After
	public void tearDown() throws Exception
	{
		if (s3MockService != null)
		{
			s3MockService.close();
		}
		if (parquetFile != null)
		{
			Files.deleteIfExists(parquetFile);
		}
	}

	@Test
	public void testMetadataAndValues() throws Exception
	{
		Assert.assertEquals(testData.getMessageType(), reader.getMessageType());
		Assert.assertEquals(testData.getRowGroups().size(), reader.getNumRowGroups());
		Assert.assertEquals(testData.getTotalRows(), reader.getTotalNumRows());
		Assert.assertEquals(testData.getDescriptor(), reader.getDescriptorByPath(COLUMN_NAME));

		for (int rowGroupIdx = 0; rowGroupIdx < testData.getRowGroups().size(); rowGroupIdx++)
		{
			Assert.assertEquals((long) testData.getRowGroups().get(rowGroupIdx).length,
					reader.getNumRowsInRowGroup(rowGroupIdx));
			double[] actual = readValues(reader, testData.getDescriptor(), rowGroupIdx);
			Assert.assertArrayEquals(testData.getRowGroups().get(rowGroupIdx), actual, 0.0d);
		}
	}

	@Test
	public void testColumnDescriptors() throws Exception
	{
		List<ColumnDescriptor> descriptors = reader.getColumnDescriptors();
		Assert.assertEquals(Collections.singletonList(testData.getDescriptor()), descriptors);
	}

	@Test
	public void testDescriptorByPath() throws Exception
	{
		Assert.assertEquals(testData.getDescriptor(), reader.getDescriptorByPath(COLUMN_NAME));
		Assert.assertEquals(testData.getDescriptor(), reader.getDescriptor(0));
	}

	@Test
	public void testNumberOfValuesInColumn() throws Exception
	{
		ColumnChunk chunk = reader.getColumnChunk(0, testData.getDescriptor());
		Assert.assertEquals(testData.getRowGroups().get(0).length, chunk.getMeta_data().getNum_values());
	}

	@Test
	public void testInputStreamSupplierProvidesFreshStreams() throws Exception
	{
		Pair<ColumnChunk, IOSupplier<InputStream>> pair = reader.getInputStreamSupplier(0, testData.getDescriptor());

		byte[] first;
		try (InputStream stream = pair.getRight().get())
		{
			first = IOUtils.toByteArray(stream);
		}

		byte[] second;
		try (InputStream stream = pair.getRight().get())
		{
			second = IOUtils.toByteArray(stream);
		}

		Assert.assertArrayEquals(first, second);
		Assert.assertEquals(testData.getRowGroups().get(0).length, pair.getLeft().getMeta_data().getNum_values());
	}



	private IndexedParquetColumnarReader createReader() throws IOException
	{
		IndexedParquetColumnarReader reader = null;
		switch (readerType)
		{
			case FILE:
				reader = ParquetFileReaderFactory.createIndexedColumnarFileReader(parquetFile);
				break;
			case S3:
				reader =  S3ParquetReaderFactory.createIndexedColumnarS3Reader(downloader);
				break;
			default:
				Assert.fail("Unhandled reader type " + readerType);
		}
		return reader;
	}

	private static double[] readValues(IndexedParquetColumnarReader reader, ColumnDescriptor descriptor, int rowGroup)
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
		return values.stream().mapToDouble(Double::doubleValue).toArray();
	}

	private static List<double[]> createDefaultRowGroups()
	{
		List<double[]> groups = new ArrayList<>();
		groups.add(new double[] { 1.0, 2.0 });
		groups.add(new double[] { 3.0, 4.0, 5.0 });
		return groups;
	}

	private enum ReaderType
	{
		FILE,
		S3
	}

	private static final class TestParquetData
	{
		private final MessageType messageType;
		private final ColumnDescriptor descriptor;
		private final List<double[]> rowGroups;
		private final long totalRows;

		private TestParquetData(MessageType messageType, ColumnDescriptor descriptor, List<double[]> rowGroups)
		{
			this.messageType = messageType;
			this.descriptor = descriptor;
			this.rowGroups = rowGroups;
			this.totalRows = rowGroups.stream().mapToLong(arr -> arr.length).sum();
		}

		static TestParquetData write(Path output, List<double[]> rowGroups) throws IOException
		{
			MessageType messageType = new MessageType("root",
					new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, COLUMN_NAME));
			ColumnDescriptor descriptor = messageType.getColumnDescription(new String[] { COLUMN_NAME });
			List<double[]> writtenGroups = new ArrayList<>(rowGroups.size());

			try (ParquetColumnarWriter writer = ParquetFileColumnarWriterFactory.createWriter(output, messageType,
					CompressionCodec.ZSTD, true))
			{
				for (double[] values : rowGroups)
				{
					double[] copy = Arrays.copyOf(values, values.length);
					writtenGroups.add(copy);
					writer.writeRowGroup(copy.length, rowGroupWriter -> rowGroupWriter.writeValues(
							chunkWriter -> chunkWriter.writeColumn(descriptor, values)));
				}
				writer.finishAndWriteFooterMetadata();
			}

			return new TestParquetData(messageType, descriptor, Collections.unmodifiableList(writtenGroups));
		}

		MessageType getMessageType()
		{
			return messageType;
		}

		ColumnDescriptor getDescriptor()
		{
			return descriptor;
		}

		List<double[]> getRowGroups()
		{
			return rowGroups;
		}

		long getTotalRows()
		{
			return totalRows;
		}
	}
}

