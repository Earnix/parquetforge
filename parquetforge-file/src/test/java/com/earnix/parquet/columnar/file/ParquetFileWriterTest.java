package com.earnix.parquet.columnar.file;

import com.earnix.parquet.columnar.file.reader.ParquetColumnarFileReader;
import com.earnix.parquet.columnar.file.reader.ParquetFileReaderFactory;
import com.earnix.parquet.columnar.file.writer.ParquetFileColumnarWriterFactory;
import com.earnix.parquet.columnar.reader.IndexedParquetColumnarReader;
import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import com.earnix.parquet.columnar.reader.chunk.InMemRowGroup;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkValuesReaderFactory;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkValuesReaderImpl;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.reader.processors.ParquetColumnarProcessors;
import com.earnix.parquet.columnar.utils.ColumnChunkForTesting;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.earnix.parquet.columnar.file.GeneralColumnReader.getValue;
import static com.earnix.parquet.columnar.file.ParquetFileFiller.fillWithRowGroups;
import static com.earnix.parquet.columnar.utils.FileUtils.processPath;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ParquetFileWriterTest
{
	private static final boolean DEBUG = false;

	@Test
	public void sanityCreateCheck() throws IOException
	{
		Path parquetFile = Files.createTempFile("testParquetFile", ".parquet");
		try
		{
			fillWithRowGroups(parquetFile);

			ParquetColumnarFileReader reader = new ParquetColumnarFileReader(parquetFile);
			reader.processFile((ParquetColumnarProcessors.ChunkProcessor) chunk -> {
				if (DEBUG)
					System.out.println(chunk.getDescriptor() + " TotalValues:" + chunk.getTotalValues());
				if (chunk.getDescriptor().getPrimitiveType().getPrimitiveTypeName()
						== PrimitiveType.PrimitiveTypeName.DOUBLE)
				{
					ChunkValuesReader colReader = ChunkValuesReaderFactory.createChunkReader(chunk);
					for (int i = 0; i < chunk.getTotalValues(); i++)
					{
						if (DEBUG)
							System.out.println(chunk.getDescriptor() + " Value: " + colReader.getDouble());
						colReader.next();
					}
				}
			});
		}
		finally
		{
			Files.deleteIfExists(parquetFile);
		}
	}

	@Test
	public void testGivenParquetFile_whenReadingAndProcessingByRowGroup_thenAllRowGroupsAreProcessed()
	{
		processPath("testParquetFile", ".parquet", path -> {
			List<RowGroupForTesting> expectedRowGroups = fillWithRowGroups(path);
			List<RowGroupForTesting> actualRowGroups = readingAndProcessByRowGroup(path);
			assertEquals(expectedRowGroups, actualRowGroups);
		});
	}

	@Test
	public void testGivenParquetFile_whenReadingChunksAndProcessingByChunkBytes_thenAllChunkAreProcessed()
	{
		processPath("testParquetFile", ".parquet", path -> {
			List<RowGroupForTesting> expectedRowGroups = fillWithRowGroups(path);
			Map<ColumnChunkForTesting, Long> chunkActualValuesToChunkActualMetadataValuesNumber = readingAndProcessByChunksBytes(
					path);

			chunkActualValuesToChunkActualMetadataValuesNumber.forEach(
					(ColumnChunkForTesting chunkValues, Long chunkValuesNumber) -> assertEquals(
							"The number of values that was read isn't the same as the metadata one, for values: "
									+ chunkValues, Long.valueOf(chunkValues.getValuesNumber()), chunkValuesNumber));

			assertAllActualAreAsExpected(chunkActualValuesToChunkActualMetadataValuesNumber,
					getChunks(expectedRowGroups));
		});
	}

	private static void assertAllActualAreAsExpected(Map<ColumnChunkForTesting, Long> actualChunks,
			List<ColumnChunkForTesting> expectedChunks)
	{
		assertEquals(actualChunks.size(), expectedChunks.size());
		actualChunks.forEach((actualChunk, actualChunkValuesNumber) -> assertActualExistsInExpectedChunks(actualChunk,
				actualChunkValuesNumber, expectedChunks));
	}

	private static void assertActualExistsInExpectedChunks(ColumnChunkForTesting actualChunk,
			Long actualChunkValuesNumber, List<ColumnChunkForTesting> expectedChunks)
	{
		if (expectedChunks.stream().noneMatch(expectedChunk -> actualChunk.equals(expectedChunk)
				&& actualChunkValuesNumber == expectedChunk.getValuesNumber()))
			fail("No match for actualChunk " + actualChunk + " or for values number");
	}

	private static List<ColumnChunkForTesting> getChunks(List<RowGroupForTesting> expectedRowGroups)
	{
		return expectedRowGroups.stream().flatMap(group -> group.getColumnChunks().stream())
				.collect(Collectors.toList());
	}

	private static List<RowGroupForTesting> readingAndProcessByRowGroup(Path parquetPath)
	{
		ParquetColumnarFileReader reader = new ParquetColumnarFileReader(parquetPath);

		List<RowGroupForTesting> actualRowGroups = new ArrayList<>();
		ParquetColumnarProcessors.RowGroupProcessor byRowGroupProcessor = rowGroup -> actualRowGroups.add(
				processByRowGroup(rowGroup));
		reader.processFile(byRowGroupProcessor);
		return actualRowGroups;
	}

	private static Map<ColumnChunkForTesting, Long> readingAndProcessByChunksBytes(Path inputParquetPath)
	{
		ParquetColumnarFileReader reader = new ParquetColumnarFileReader(inputParquetPath);

		Map<ColumnChunkForTesting, Long> chunkActualValuesToChunkActualMetadataValuesNumber = new HashMap<>();

		ParquetColumnarProcessors.ProcessRawChunkBytes byChunkProcessor = (ColumnDescriptor descriptor, ColumnChunk columnChunk, InputStream chunkInput) -> {
			ColumnChunkForTesting chunk = createFileAndWriteChunkByBytesAndReadItByValues(descriptor, chunkInput,
					columnChunk);
			chunkActualValuesToChunkActualMetadataValuesNumber.put(chunk, columnChunk.getMeta_data().getNum_values());
		};

		reader.processFile(byChunkProcessor);

		return chunkActualValuesToChunkActualMetadataValuesNumber;
	}

	private static ColumnChunkForTesting createFileAndWriteChunkByBytesAndReadItByValues(ColumnDescriptor descriptor,
			InputStream chunkInput, ColumnChunk columnChunk)
	{
		AtomicReference<ColumnChunkForTesting> columnChunkForTesting = new AtomicReference<>();
		processPath("testCopyOfChunksOutputFile", ".parquet", outputPath -> columnChunkForTesting.set(
				writeChunkByBytesAndReadItByValues(descriptor, chunkInput, columnChunk, outputPath)));
		return columnChunkForTesting.get();
	}

	private static ColumnChunkForTesting writeChunkByBytesAndReadItByValues(ColumnDescriptor descriptor,
			InputStream chunkInput, ColumnChunk columnChunk, Path outputPath)
	{
		writeChunkAsOnlyOneInOutputParquetFile(outputPath, descriptor, chunkInput, columnChunk);
		List<ColumnChunkForTesting> chunks = readAllChunksFromOutput(outputPath);
		assertEquals("One chunk was written, so one should have been read as well", 1, chunks.size());
		return chunks.get(0);
	}

	private static List<ColumnChunkForTesting> readAllChunksFromOutput(Path outputPath)
	{
		return getChunks(readingAndProcessByRowGroup(outputPath));
	}

	private static void writeChunkAsOnlyOneInOutputParquetFile(Path outputPath, ColumnDescriptor descriptor,
			InputStream chunkInput, ColumnChunk columnChunk)
	{
		try (ParquetColumnarWriter parquetColumnarWriter = ParquetFileColumnarWriterFactory.createWriter(outputPath,
				asList(descriptor.getPrimitiveType()), false))
		{
			parquetColumnarWriter.writeRowGroup(columnChunk.getMeta_data().getNum_values(),
					rowGroupWriter -> rowGroupWriter.writeCopyOfChunk(descriptor, columnChunk, chunkInput));
			parquetColumnarWriter.finishAndWriteFooterMetadata();
		}
		catch (IOException ex)
		{
			throw new UncheckedIOException(ex);
		}
	}

	private static RowGroupForTesting processByRowGroup(InMemRowGroup rowGroup)
	{
		RowGroupForTesting rowGroupForTesting = new RowGroupForTesting(rowGroup.getNumRows());
		Consumer<InMemChunk> chunkProcessor = chunk -> rowGroupForTesting.addChunk(processChunk(chunk));
		rowGroup.forEachColumnChunk(chunkProcessor);
		return rowGroupForTesting;
	}

	private static ColumnChunkForTesting processChunk(InMemChunk chunk)
	{
		ChunkValuesReaderImpl colReader = new ChunkValuesReaderImpl(chunk);
		ColumnDescriptor descriptor = chunk.getDescriptor();
		return new ColumnChunkForTesting(descriptor, getChunkValues(descriptor, chunk, colReader));
	}

	private static List<Object> getChunkValues(ColumnDescriptor columnDescriptor, InMemChunk chunk,
			ChunkValuesReader colReader)
	{
		List<Object> ret = new ArrayList<>(Math.toIntExact(chunk.getTotalValues()));

		do
		{
			ret.add(getValue(colReader, columnDescriptor));
		}
		while (colReader.next());

		return ret;
	}


	/**
	 * The C++ parquet driver assumes that column ordering in row groups is in identical order of
	 * {@link org.apache.parquet.format.SchemaElement} This tests checks that it is reordered as expected.
	 */
	@Test
	public void testSchemaOrdering() throws IOException
	{
		Path parquetFile = Files.createTempFile("parquetfile", ".parquet");

		List<PrimitiveType> cols = asList(
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, "double"),
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, "boolean"));
		MessageType messageType = new MessageType("root", Collections.unmodifiableList(cols));
		try
		{
			try (ParquetColumnarWriter rowGroupWriter = ParquetFileColumnarWriterFactory.createWriter(parquetFile, cols,
					false))
			{

				rowGroupWriter.writeRowGroup(1, rgw -> {
					// write columns in order opposite to the schema.
					rgw.writeValues(
							writer -> writer.writeColumn(messageType.getColumnDescription(new String[] { "boolean" }),
									new boolean[] { true }));
					rgw.writeValues(
							writer -> writer.writeColumn(messageType.getColumnDescription(new String[] { "double" }),
									new double[] { 3 }));
				});
				rowGroupWriter.finishAndWriteFooterMetadata();
			}
			ParquetColumnarFileReader reader = new ParquetColumnarFileReader(parquetFile);
			FileMetaData md = reader.readMetaData();
			Assert.assertEquals("root", md.schema.get(0).getName());
			Assert.assertEquals("double", md.schema.get(1).getName());
			Assert.assertEquals("boolean", md.schema.get(2).getName());

			RowGroup rowGroup = md.getRow_groups().get(0);
			Assert.assertEquals(singletonList("double"),
					rowGroup.getColumns().get(0).getMeta_data().getPath_in_schema());
			Assert.assertEquals(singletonList("boolean"),
					rowGroup.getColumns().get(1).getMeta_data().getPath_in_schema());
		}
		finally
		{
			Files.deleteIfExists(parquetFile);
		}
	}

	/**
	 * Test float datatype
	 */
	@Test
	public void testFloatColumn() throws IOException
	{
		Path parquetFile = Files.createTempFile("parquetfile", ".parquet");

		List<PrimitiveType> cols = asList(
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.FLOAT, "float"));
		MessageType messageType = new MessageType("root", Collections.unmodifiableList(cols));
		try
		{
			try (ParquetColumnarWriter rowGroupWriter = ParquetFileColumnarWriterFactory.createWriter(parquetFile, cols,
					false))
			{

				rowGroupWriter.writeRowGroup(2, rgw -> {
					// write columns in order opposite to the schema.
					rgw.writeValues(
							writer -> writer.writeColumn(messageType.getColumnDescription(new String[] { "float" }),
									new float[] { 1f, .1f }));
				});
				rowGroupWriter.finishAndWriteFooterMetadata();
			}
			IndexedParquetColumnarReader reader = ParquetFileReaderFactory.createIndexedColumnarFileReader(parquetFile);

			InMemChunk inMemChunk = reader.readInMem(0, messageType.getColumnDescription(new String[] { "float" }));
			ChunkValuesReader chunkReader = ChunkValuesReaderFactory.createChunkReader(inMemChunk);
			Assert.assertEquals(1f, chunkReader.getFloat(), 1e-15);
			Assert.assertTrue(chunkReader.next());
			Assert.assertEquals(.1f, chunkReader.getFloat(), 1e-15);
		}
		finally
		{
			Files.deleteIfExists(parquetFile);
		}
	}

	/**
	 * Test fixed len binary datatype
	 */
	@Test
	public void testFixedLenBinColumn() throws IOException
	{
		Path parquetFile = Files.createTempFile("parquetfile", ".parquet");

		int fixBinLen = 4;
		List<PrimitiveType> cols = asList(
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
						fixBinLen, "fixed_len"));
		MessageType messageType = new MessageType("root", Collections.unmodifiableList(cols));
		try
		{
			try (ParquetColumnarWriter rowGroupWriter = ParquetFileColumnarWriterFactory.createWriter(parquetFile, cols,
					false))
			{

				rowGroupWriter.writeRowGroup(2, rgw -> {
					// write columns in order opposite to the schema.
					rgw.writeValues(
							writer -> writer.writeColumn(messageType.getColumnDescription(new String[] { "fixed_len" }),
									new String[] { "abcd", "efgh" }));
				});
				rowGroupWriter.finishAndWriteFooterMetadata();
			}

			IndexedParquetColumnarReader reader = ParquetFileReaderFactory.createIndexedColumnarFileReader(parquetFile);
			Assert.assertEquals(fixBinLen,
					reader.getMessageType().getColumns().get(0).getPrimitiveType().getTypeLength());
			InMemChunk inMemChunk = reader.readInMem(0, messageType.getColumnDescription(new String[] { "fixed_len" }));
			ChunkValuesReader chunkReader = ChunkValuesReaderFactory.createChunkReader(inMemChunk);
			Assert.assertEquals("abcd", chunkReader.getBinary().toStringUsingUTF8());
			Assert.assertTrue(chunkReader.next());
			Assert.assertEquals("efgh", chunkReader.getBinary().toStringUsingUTF8());
		}
		finally
		{
			Files.deleteIfExists(parquetFile);
		}
	}
}
