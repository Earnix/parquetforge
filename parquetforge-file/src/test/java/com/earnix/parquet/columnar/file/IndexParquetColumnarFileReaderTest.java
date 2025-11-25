package com.earnix.parquet.columnar.file;

import com.earnix.parquet.columnar.file.reader.ParquetFileReaderFactory;
import com.earnix.parquet.columnar.file.writer.ParquetFileColumnarWriterFactory;
import com.earnix.parquet.columnar.reader.IndexedParquetColumnarReader;
import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkValuesReaderFactory;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import org.apache.commons.io.FileUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for reading arbitrary column chunks form a parquet file
 */
public class IndexParquetColumnarFileReaderTest
{
	private Path tmpFolder;

	@Before
	public void setUp() throws Exception
	{
		tmpFolder = Files.createTempDirectory("index_parquet_test");
	}

	@After
	public void tearDown() throws Exception
	{
		FileUtils.forceDelete(tmpFolder.toFile());
	}

	/**
	 * A simple test of the indexed parquet file reader
	 *
	 * @throws Exception
	 */
	@Test
	public void simpleTest() throws Exception
	{
		Path parquetFile = tmpFolder.resolve("simple.parquet");

		// write a simple parquet file with 2 columns and two row groups each with one row.
		List<PrimitiveType> cols = Arrays.asList(
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "col1"),
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "col2"));
		MessageType messageType = new MessageType("root", new ArrayList<>(cols));
		List<ColumnDescriptor> colDescriptors = messageType.getColumns();
		try (ParquetColumnarWriter fileWriter = ParquetFileColumnarWriterFactory.createWriter(parquetFile, cols,
				CompressionCodec.ZSTD, true))
		{
			fileWriter.writeRowGroup(1, writer -> {
				writer.writeValues(colWriter -> colWriter.writeColumn(colDescriptors.get(0), new int[] { 0 }));
				writer.writeValues(colWriter -> colWriter.writeColumn(colDescriptors.get(1), new int[] { 1 }));
			});

			fileWriter.writeRowGroup(1, writer -> {
				writer.writeValues(colWriter -> colWriter.writeColumn(colDescriptors.get(0), new int[] { 2 }));
				writer.writeValues(colWriter -> colWriter.writeColumn(colDescriptors.get(1), new int[] { 3 }));
			});

			fileWriter.finishAndWriteFooterMetadata();
		}

		IndexedParquetColumnarReader fileReader = ParquetFileReaderFactory.createIndexedColumnarFileReader(parquetFile);

		assertExpected(fileReader, 0, 0, new int[] { 0 });
		assertExpected(fileReader, 1, 0, new int[] { 2 });
		assertExpected(fileReader, 0, 1, new int[] { 1 });
		assertExpected(fileReader, 1, 1, new int[] { 3 });
	}

	private static void assertExpected(IndexedParquetColumnarReader fileReader, int rowGroup, int colOffset,
			int[] expected) throws IOException
	{
		InMemChunk chunk = fileReader.readInMem(rowGroup, fileReader.getDescriptor(colOffset));
		ChunkValuesReader chunkValuesReader = ChunkValuesReaderFactory.createChunkReader(chunk);
		for (int i = 0; i < expected.length; i++)
		{
			Assert.assertEquals(expected[i], chunkValuesReader.getInteger());
			boolean notLastElement = i < expected.length - 1;
			Assert.assertEquals(notLastElement, chunkValuesReader.next());
		}
		Assert.assertFalse(chunkValuesReader.next());
	}
}
