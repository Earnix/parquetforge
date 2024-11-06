package com.earnix.parquet.columnar.file;

import com.earnix.parquet.columnar.reader.ParquetColumarFileReader;
import com.earnix.parquet.columnar.reader.chunk.InMemRowGroup;
import com.earnix.parquet.columnar.reader.chunk.internal.HackyParquetExtendedColumnReader;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.reader.processors.ParquetColumnarProcessors;
import com.earnix.parquet.columnar.utils.ColumnChunkForTesting;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import com.earnix.parquet.columnar.writer.ParquetFileColumnarWriterImpl;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.earnix.parquet.columnar.file.GeneralColumnReader.getValue;
import static com.earnix.parquet.columnar.file.ParquetFileFiller.fillWithRowGroups;
import static com.earnix.parquet.columnar.utils.FileUtils.processPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ParquetFileWriterTest
{
	@Test
	public void sanityCreateCheck() throws IOException
	{
		Path parquetFile = Files.createTempFile("testParquetFile", ".parquet");
		try
		{
			fillWithRowGroups(parquetFile);

			ParquetColumarFileReader reader = new ParquetColumarFileReader(parquetFile);
			reader.processFile((ParquetColumnarProcessors.ChunkProcessor) chunk -> {
				System.out.println(chunk.getDescriptor() + " TotalValues:" + chunk.getTotalValues());
				if (chunk.getDescriptor().getPrimitiveType()
						.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.DOUBLE)
				{
					ColumnReaderImpl colReader = new HackyParquetExtendedColumnReader(chunk);
					for (int i = 0; i < chunk.getTotalValues(); i++)
					{
						colReader.consume();
						System.out.println(chunk.getDescriptor() + " Value: " + colReader.getDouble());
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
			Map<ColumnChunkForTesting, Long> chunkActualValuesToChunkActualMetadataValuesNumber = readingAndProcessByChunksBytes(path);

			chunkActualValuesToChunkActualMetadataValuesNumber.forEach((ColumnChunkForTesting chunkValues, Long chunkValuesNumber) ->
					assertEquals("The number of values that was read isn't the same as the metadata one, for values: " + chunkValues, Long.valueOf(chunkValues.getValuesNumber()), chunkValuesNumber));

			assertAllActualAreAsExpected(chunkActualValuesToChunkActualMetadataValuesNumber, getChunks(expectedRowGroups));
		});
	}

	private static void assertAllActualAreAsExpected(Map<ColumnChunkForTesting, Long> actualChunks, List<ColumnChunkForTesting> expectedChunks)
	{
		assertEquals(actualChunks.size(), expectedChunks.size());
		actualChunks.forEach((actualChunk, actualChunkValuesNumber) ->
				assertActualExistsInExpectedChunks(actualChunk, actualChunkValuesNumber, expectedChunks)
		);
	}

	private static void assertActualExistsInExpectedChunks(ColumnChunkForTesting actualChunk, Long actualChunkValuesNumber, List<ColumnChunkForTesting> expectedChunks)
	{
		if (expectedChunks.stream()
				.noneMatch(expectedChunk -> actualChunk.equals(expectedChunk) && actualChunkValuesNumber == expectedChunk.getValuesNumber()))
			fail("No match for actualChunk " + actualChunk + " or for values number");
	}

	private static List<ColumnChunkForTesting> getChunks(List<RowGroupForTesting> expectedRowGroups)
	{
		return expectedRowGroups.stream()
				.flatMap(group -> group.getColumnChunks().stream())
				.collect(Collectors.toList());
	}

	private static List<RowGroupForTesting> readingAndProcessByRowGroup(Path parquetPath)
	{
		ParquetColumarFileReader reader = new ParquetColumarFileReader(parquetPath);

		List<RowGroupForTesting> actualRowGroups = new ArrayList<>();
		ParquetColumnarProcessors.RowGroupProcessor byRowGroupProcessor = rowGroup -> actualRowGroups.add(processByRowGroup(rowGroup));
		reader.processFile(byRowGroupProcessor);
		return actualRowGroups;
	}

	private static Map<ColumnChunkForTesting, Long> readingAndProcessByChunksBytes(Path inputParquetPath)
	{
		ParquetColumarFileReader reader = new ParquetColumarFileReader(inputParquetPath);

		Map<ColumnChunkForTesting, Long> chunkActualValuesToChunkActualMetadataValuesNumber = new HashMap<>();

		ParquetColumnarProcessors.ProcessRawChunkBytes byChunkProcessor =
				(ColumnDescriptor descriptor, ColumnChunk columnChunk, InputStream chunkInput) -> {
					ColumnChunkForTesting chunk = createFileAndWriteChunkByBytesAndReadItByValues(descriptor, chunkInput, columnChunk);
					chunkActualValuesToChunkActualMetadataValuesNumber.put(chunk, columnChunk.getMeta_data().getNum_values());
				};

		reader.processFile(byChunkProcessor);

		return chunkActualValuesToChunkActualMetadataValuesNumber;
	}

	private static ColumnChunkForTesting createFileAndWriteChunkByBytesAndReadItByValues(ColumnDescriptor descriptor, InputStream chunkInput, ColumnChunk columnChunk)
	{
		AtomicReference<ColumnChunkForTesting> columnChunkForTesting = new AtomicReference<>();
		processPath("testCopyOfChunksOutputFile", ".parquet", outputPath ->
				columnChunkForTesting.set(writeChunkByBytesAndReadItByValues(descriptor, chunkInput, columnChunk, outputPath))
		);
		return columnChunkForTesting.get();
	}

	private static ColumnChunkForTesting writeChunkByBytesAndReadItByValues(ColumnDescriptor descriptor, InputStream chunkInput, ColumnChunk columnChunk, Path outputPath)
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

	private static void writeChunkAsOnlyOneInOutputParquetFile(Path outputPath, ColumnDescriptor descriptor, InputStream chunkInput, ColumnChunk columnChunk){
		try (ParquetColumnarWriter parquetColumnarWriter = new ParquetFileColumnarWriterImpl(outputPath, Arrays.asList(descriptor.getPrimitiveType())))
		{
			parquetColumnarWriter.processRowGroup(columnChunk.getMeta_data().getNum_values(), rowGroupWriter ->
					rowGroupWriter.writeCopyOfChunk(descriptor, columnChunk, chunkInput)
			);
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
		ColumnReaderImpl colReader = new HackyParquetExtendedColumnReader(chunk);
		ColumnDescriptor descriptor = chunk.getDescriptor();
		return new ColumnChunkForTesting(
				descriptor.getPrimitiveType().getName(),
				getChunkValues(descriptor, chunk, colReader));
	}

	private static List<Object> getChunkValues(ColumnDescriptor columnDescriptor, InMemChunk chunk, ColumnReaderImpl colReader)
	{
		return LongStream.range(0, chunk.getTotalValues())
				.mapToObj(index -> getValue(colReader, columnDescriptor))
				.collect(Collectors.toList());
	}

}
