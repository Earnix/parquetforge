package com.earnix.parquet.columnar.file;

import com.earnix.parquet.columnar.file.reader.ParquetFileReaderFactory;
import com.earnix.parquet.columnar.reader.IndexedParquetColumnarReader;
import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkValuesReaderFactory;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.google.common.collect.Lists;
import com.google.common.math.IntMath;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.earnix.parquet.columnar.file.ChunkValuesReaderTest.assertEquals;
import static com.earnix.parquet.columnar.file.ChunkValuesReaderTest.writeValues;

/**
 * Tests construction of chunk values readers that skip rows
 */
public class ChunkValuesSkipTest
{
	private final String colName = "chicken";
	private final MessageType messageType = new MessageType("root",
			new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, colName));
	private final Integer[] vals = { 1, null, null, 2, 3, 4, 5, 6, 7, 8, 9 };
	private Path tmpFile;

	@Before
	public void setUp() throws Exception
	{
		tmpFile = Files.createTempFile("parquetforge", ".parquet");
	}

	@After
	public void tearDown() throws Exception
	{
		Files.deleteIfExists(tmpFile);
		tmpFile = null;
	}

	@Test
	public void testSkipRows() throws Exception
	{
		writeParquetFile();
		IndexedParquetColumnarReader reader = ParquetFileReaderFactory.createIndexedColumnarFileReader(tmpFile);
		Assert.assertEquals(1, reader.getNumRowGroups());
		InMemChunk inMemChunk = reader.readInMem(0, messageType.getColumnDescription(new String[] { colName }));


		for (int startRow = 0; startRow < vals.length - 1; startRow++)
		{
			ChunkValuesReader chunkValuesReader = ChunkValuesReaderFactory.createChunkReader(inMemChunk, startRow);
			for (int i = startRow; i < vals.length; i++)
			{
				assertEquals(vals[i], chunkValuesReader);
				Assert.assertEquals(i < vals.length - 1, chunkValuesReader.next());
			}
		}

		int dataPageCount;
		var dataPages = inMemChunk.getDataPages();
		for (dataPageCount = 0; dataPages.readPage() != null; dataPageCount++)
			;
		Assert.assertEquals(IntMath.divide(vals.length, 2, RoundingMode.CEILING), dataPageCount);
	}

	private void writeParquetFile() throws Exception
	{

		// force a new page at every 2 rows
		ParquetProperties writeProperties = ParquetProperties.builder()//
				.withPageRowCountLimit(2)//
				.withPageValueCountThreshold(2)//
				.withMinRowCountForPageSizeCheck(2)//
				.withMaxRowCountForPageSizeCheck(2)//
				.build();

		writeValues(tmpFile, messageType, writeProperties, vals);

	}
}
