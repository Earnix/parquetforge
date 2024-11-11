package com.earnix.parquet.columnar.s3.randomized;

import com.earnix.parquet.columnar.reader.ParquetColumarFileReader;
import com.earnix.parquet.columnar.reader.chunk.internal.HackyParquetExtendedColumnReader;
import com.earnix.parquet.columnar.reader.processors.ParquetColumnarProcessors;
import com.earnix.parquet.columnar.s3.ParquetS3ObjectWriterImpl;
import com.earnix.parquet.columnar.s3.S3MockService;
import com.earnix.parquet.columnar.s3.buffering.S3KeyUploader;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import com.earnix.parquet.columnar.writer.ParquetFileInfo;
import org.apache.commons.io.FileUtils;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

@RunWith(Parameterized.class)
public class RandomizedDataTest
{
	private static final String KEY = "example.parquet";
	private static final int[] NUM_ROWS = new int[] { 1, 2, 4, 7, 10, 100, 10_000, 1_000_000, 5_000_000 };
	private static final int[] ROW_GROUP_LEN = new int[] { 100_000, 1_000_000, 3_000_000 };
	private static final int[] TARGET_PARTS_PER_ROWGRP = new int[] { 1, 2, 3 };
	private static final int[] NUM_COLS = new int[] { 1, 2, 5 };

	@Parameterized.Parameters(name = "size = {0}, rowGrpLen = {1} targetPartsPerGrp = {2} numCols = {3}")
	public static Collection<Object[]> getParameters()
	{
		List<Object[]> ret = new ArrayList<>();

		for (int numRows : NUM_ROWS)
		{
			int rowGrpLenIdx = 0;
			do
			{
				for (int targetPartPerRowGrp : TARGET_PARTS_PER_ROWGRP)
				{
					for (int numCols : NUM_COLS)
					{
						// no reason to add a higher target parts per row if we know we can't meet it..
						if (numCols >= targetPartPerRowGrp)
						{
							ret.add(new Object[] { numRows, ROW_GROUP_LEN[rowGrpLenIdx], targetPartPerRowGrp,
									numCols });
						}
					}
				}
				rowGrpLenIdx++;
			}
			while (rowGrpLenIdx < ROW_GROUP_LEN.length && numRows > ROW_GROUP_LEN[rowGrpLenIdx - 1]);
		}

		return ret;
	}

	@BeforeClass
	public static void beforeClass() throws Exception
	{
		s3MockService = new S3MockService();
	}

	@AfterClass
	public static void afterClass() throws Exception
	{
		s3MockService.close();
	}

	@Before
	public void setUp() throws Exception
	{
		tmpFolder = Files.createTempDirectory("randomized_parquet_test");
		resetRandom();
	}

	private void resetRandom()
	{
		random = new ConcurrentHashMap<>();
	}

	private Random getRandom(String colName)
	{
		return random.computeIfAbsent(colName, c -> new Random(getBaseSeed() ^ c.hashCode()));
	}

	private int getBaseSeed()
	{
		return numRows ^ rowGrpLen;
	}

	@After
	public void tearDown() throws Exception
	{
		s3MockService.resetTestBucket();
		FileUtils.forceDelete(tmpFolder.toFile());
	}

	private static S3MockService s3MockService;
	private final int numRows;
	private final int rowGrpLen;
	private final int targetPartsPerRowGrp;
	private final int numCols;
	private Path tmpFolder;
	// column name to random
	private Map<String, Random> random;

	public RandomizedDataTest(int numRows, int rowGrpLen, int targetPartsPerRowGrp, int numCols)
	{
		this.numRows = numRows;
		this.rowGrpLen = rowGrpLen;
		this.targetPartsPerRowGrp = targetPartsPerRowGrp;
		this.numCols = numCols;
	}

	@Test
	public void testDoubleCol() throws Exception
	{
		ParquetFileInfo info = runDoubleTest();
		System.out.printf(
				"numRows: %d rowGrpSize: %d fileSize: %d numRowGroups: %d targetPartsPerRowGrp: %d numCols %d\n",
				numRows, rowGrpLen, info.getTotalParquetFileSize(), info.getFileMetaData().getRow_groupsSize(),
				targetPartsPerRowGrp, numCols);
		Path downloaded = downloadToTempFile(KEY);

		resetRandom();
		readAndAssertValues(downloaded, info);
	}

	private double[] getRandomVals(String colName, int numValues)
	{
		double[] randDoubleVals = new double[numValues];
		Random rand = getRandom(colName);
		for (int i = 0; i < randDoubleVals.length; i++)
		{
			randDoubleVals[i] = rand.nextDouble();
		}
		return randDoubleVals;
	}

	private void readAndAssertValues(Path downloaded, ParquetFileInfo info) throws IOException
	{
		ParquetColumarFileReader reader = new ParquetColumarFileReader(downloaded);
		Assert.assertEquals(info.getFileMetaData().getRow_groupsSize(), reader.readMetaData().getRow_groupsSize());

		// this assumes the implementation of process file is single and in order. Seems not great.
		reader.processFile((ParquetColumnarProcessors.RowGroupProcessor) rowGroup -> {
			Assert.assertTrue(rowGroup.getNumRows() <= rowGrpLen);
			rowGroup.forEachColumnChunk(inMemChunk -> {
				HackyParquetExtendedColumnReader columnReader = new HackyParquetExtendedColumnReader(inMemChunk);
				double[] vals = getRandomVals(inMemChunk.getDescriptor().getPath()[0],
						(int) inMemChunk.getTotalValues());
				for (int i = 0; i < rowGroup.getNumRows(); i++)
				{
					columnReader.consume();
					Assert.assertEquals(vals[i], columnReader.getDouble(), 0d);
				}
			});
		});
	}

	private ParquetFileInfo runDoubleTest() throws Exception
	{
		S3Client s3Client = s3MockService.getS3Client();
		S3KeyUploader uploader = new S3KeyUploader(s3Client, s3MockService.testBucket(), KEY);
		String colName = "testDouble";
		MessageType messageType = new MessageType("root",
				IntStream.range(0, numCols).mapToObj(i -> getColumnName(colName, i))
						.map(name -> new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.DOUBLE,
								name)).toArray(Type[]::new));
		ParquetProperties properties = ParquetProperties.builder().build();

		try (ParquetColumnarWriter writer = new ParquetS3ObjectWriterImpl(messageType, CompressionCodec.ZSTD,
				properties, uploader, targetPartsPerRowGrp))
		{
			for (int rangeStart = 0; rangeStart < numRows; rangeStart += rowGrpLen)
			{
				int numVals = Math.min(rowGrpLen, numRows - rangeStart);
				writer.writeRowGroup(numVals, rowGroupWriter -> {
					for (int i = 0; i < numCols; i++)
					{
						String columnName = getColumnName(colName, i);
						double[] valsToUpload = getRandomVals(columnName, numVals);
						rowGroupWriter.writeValues(chunkWriter -> chunkWriter.writeColumn(columnName, valsToUpload));
					}
				});
			}
			return writer.finishAndWriteFooterMetadata();
		}
	}

	private static String getColumnName(String colName, int i)
	{
		return colName + i;
	}

	private Path downloadToTempFile(String key) throws IOException
	{
		Path tmpFile = Files.createTempFile(tmpFolder, "potato", ".parquet");
		try (InputStream resp = s3MockService.getS3Client()
				.getObject(builder -> builder.bucket(s3MockService.testBucket()).key(key)))
		{
			Files.copy(resp, tmpFile, StandardCopyOption.REPLACE_EXISTING);
		}
		return tmpFile;
	}
}
