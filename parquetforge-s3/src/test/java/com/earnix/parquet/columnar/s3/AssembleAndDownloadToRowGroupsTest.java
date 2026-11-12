package com.earnix.parquet.columnar.s3;

import com.earnix.parquet.columnar.assembler.ParquetRowGroupSupplier;
import com.earnix.parquet.columnar.file.assembler.ParquetFileChunkSupplier;
import com.earnix.parquet.columnar.file.reader.ParquetColumnarFileReader;
import com.earnix.parquet.columnar.file.reader.ParquetFileReaderFactory;
import com.earnix.parquet.columnar.file.writer.ParquetFileColumnarWriterFactory;
import com.earnix.parquet.columnar.reader.IndexedParquetColumnarReader;
import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkValuesReaderFactory;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.s3.assembler.S3ParquetAssembleAndUpload;
import com.earnix.parquet.columnar.s3.buffering.S3KeyUploader;
import com.earnix.parquet.columnar.s3.downloader.S3KeyDownloader;
import com.earnix.parquet.columnar.s3.downloader.S3ParquetFilePartDownloader;
import com.earnix.parquet.columnar.utils.ParquetMagicUtils;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import org.apache.commons.io.file.PathUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Assemble local row groups into one S3 object with {@link S3ParquetAssembleAndUpload}, then split it back into one
 * file per row group with {@link S3ParquetFilePartDownloader}. The row groups have different row counts on purpose, so
 * that a footer carrying the wrong total row count is detected.
 */
public class AssembleAndDownloadToRowGroupsTest
{
	private static final String COL_NAME = "testDouble";
	private static final String ASSEMBLED_KEY = "assembled.parquet";
	private static final String WRITTEN_KEY = "written.parquet";
	private static final double[][] ROW_GROUP_VALUES = { { 1, 2 }, { 3, 4, 5 } };

	private final MessageType messageType = new MessageType("root",
			new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE, COL_NAME));
	private final ColumnDescriptor descriptor = messageType.getColumnDescription(new String[] { COL_NAME });

	private Path tmpDir;
	private S3MockService service;
	private S3Client s3Client;

	@Before
	public void setUp() throws Exception
	{
		tmpDir = Files.createTempDirectory("assemble-download");
		service = new S3MockService();
		s3Client = service.getS3Client();
		assembleAndUpload(writeOneFilePerRowGroup());
	}

	@After
	public void tearDown() throws IOException
	{
		service.close();
		PathUtils.delete(tmpDir);
	}

	/** Each row group's file_offset must point to its first column chunk, i.e. where the previous row group ended */
	@Test
	public void assembledFooterRowGroupFileOffsetIsRowGroupStart()
	{
		FileMetaData footer = new S3ParquetFilePartDownloader(downloader(ASSEMBLED_KEY)).getFileMetadata();
		long expectedStart = ParquetMagicUtils.PARQUET_MAGIC.length();
		for (RowGroup rowGroup : footer.getRow_groups())
		{
			Assert.assertEquals(expectedStart, rowGroup.getFile_offset());
			Assert.assertEquals(expectedStart, rowGroup.getColumns().get(0).getMeta_data().getData_page_offset());
			expectedStart += rowGroup.getTotal_compressed_size();
		}
	}

	/** Download of the assembled object: every row group file must hold exactly that row group's rows */
	@Test
	public void downloadToRowGroupsOfAssembledObjectWritesReadableFilePerRowGroup() throws Exception
	{
		downloadAndValidateRowGroupFiles(ASSEMBLED_KEY);
	}

	/**
	 * Same download, of an object written by {@link ParquetS3ObjectWriterImpl} (correct row group offsets), so only the
	 * footer of the downloaded files is tested.
	 */
	@Test
	public void downloadToRowGroupsOfWrittenObjectWritesReadableFilePerRowGroup() throws Exception
	{
		writeObjectDirectly();
		downloadAndValidateRowGroupFiles(WRITTEN_KEY);
	}

	private void downloadAndValidateRowGroupFiles(String key) throws Exception
	{
		List<Path> rowGroupFiles = new ArrayList<>();
		new S3ParquetFilePartDownloader(downloader(key)).downloadToRowGroups((rowGroupNum, rowGroup) -> {
			Path p = tmpDir.resolve(key + "-" + rowGroupNum + ".parquet");
			rowGroupFiles.add(p);
			return p;
		});

		Assert.assertEquals(ROW_GROUP_VALUES.length, rowGroupFiles.size());
		for (int i = 0; i < ROW_GROUP_VALUES.length; i++)
		{
			IndexedParquetColumnarReader reader = ParquetFileReaderFactory.createIndexedColumnarFileReader(
					rowGroupFiles.get(i));
			Assert.assertEquals(1, reader.getNumRowGroups());
			Assert.assertEquals(ROW_GROUP_VALUES[i].length, reader.getNumRowsInRowGroup(0));
			Assert.assertEquals(ROW_GROUP_VALUES[i].length,
					new ParquetColumnarFileReader(rowGroupFiles.get(i)).readMetaData().getNum_rows());
			assertValues(reader, ROW_GROUP_VALUES[i]);
		}
	}

	private void writeObjectDirectly() throws IOException
	{
		S3KeyUploader uploader = new S3KeyUploader(s3Client, service.testBucket(), WRITTEN_KEY);
		try (ParquetColumnarWriter writer = new ParquetS3ObjectWriterImpl(messageType, CompressionCodec.ZSTD,
				ParquetProperties.builder().build(), uploader, ROW_GROUP_VALUES.length))
		{
			for (double[] values : ROW_GROUP_VALUES)
			{
				writer.writeRowGroup(values.length, rowGroupWriter -> rowGroupWriter.writeValues(
						chunkWriter -> chunkWriter.writeColumn(descriptor, values)));
			}
			writer.finishAndWriteFooterMetadata();
		}
	}

	private List<Path> writeOneFilePerRowGroup() throws IOException
	{
		List<Path> files = new ArrayList<>();
		for (int i = 0; i < ROW_GROUP_VALUES.length; i++)
		{
			Path file = tmpDir.resolve("local-" + i + ".parquet");
			double[] values = ROW_GROUP_VALUES[i];
			try (ParquetColumnarWriter writer = ParquetFileColumnarWriterFactory.createWriter(file, messageType,
					CompressionCodec.ZSTD, true))
			{
				writer.writeRowGroup(values.length, rowGroupWriter -> rowGroupWriter.writeValues(
						chunkWriter -> chunkWriter.writeColumn(descriptor, values)));
				writer.finishAndWriteFooterMetadata();
			}
			files.add(file);
		}
		return files;
	}

	private void assembleAndUpload(List<Path> localFiles) throws IOException
	{
		List<ParquetRowGroupSupplier> rowGroups = new ArrayList<>();
		for (Path file : localFiles)
		{
			IndexedParquetColumnarReader reader = ParquetFileReaderFactory.createIndexedColumnarFileReader(file);
			rowGroups.add(ParquetRowGroupSupplier.builder()
					.addChunkSupplier(new ParquetFileChunkSupplier(reader, reader.getDescriptor(0), 0)).build());
		}
		S3KeyUploader uploader = new S3KeyUploader(s3Client, service.testBucket(), ASSEMBLED_KEY);
		new S3ParquetAssembleAndUpload(messageType, List.of(), rowGroups.size(), 1).assembleAndUpload(uploader,
				rowGroups);
	}

	private S3KeyDownloader downloader(String key)
	{
		return new S3KeyDownloader(s3Client, service.testBucket(), key);
	}

	private void assertValues(IndexedParquetColumnarReader reader, double[] expected) throws IOException
	{
		InMemChunk chunk = reader.readInMem(0, descriptor);
		Assert.assertEquals(expected.length, chunk.getTotalValues());
		ChunkValuesReader valReader = ChunkValuesReaderFactory.createChunkReader(chunk);
		for (int i = 0; i < expected.length; i++)
		{
			Assert.assertEquals(expected[i], valReader.getDouble(), 0.0d);
			Assert.assertEquals(i < expected.length - 1, valReader.next());
		}
	}
}
