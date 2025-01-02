package com.earnix.parquet.columnar.s3.randomized;

import com.earnix.parquet.columnar.reader.IndexedParquetColumnarFileReader;
import com.earnix.parquet.columnar.reader.ParquetColumnarFileReader;
import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import com.earnix.parquet.columnar.reader.chunk.InMemRowGroup;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkValuesReaderFactory;
import com.earnix.parquet.columnar.reader.processors.ParquetColumnarProcessors;
import com.earnix.parquet.columnar.s3.ParquetS3ObjectWriterImpl;
import com.earnix.parquet.columnar.s3.S3MockService;
import com.earnix.parquet.columnar.s3.buffering.S3KeyUploader;
import com.earnix.parquet.columnar.s3.downloader.S3KeyDownloader;
import com.earnix.parquet.columnar.s3.downloader.S3ParquetFilePartDownloader;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import com.earnix.parquet.columnar.writer.ParquetFileInfo;
import com.google.common.math.IntMath;
import com.google.common.primitives.Ints;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.file.PathUtils;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
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
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
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
	private final int numRowGroups;
	private final int targetPartsPerRowGrp;
	private final int numCols;
	private Path tmpFolder;
	// column name to random
	private Map<String, Random> random;

	public RandomizedDataTest(int numRows, int rowGrpLen, int targetPartsPerRowGrp, int numCols)
	{
		this.numRows = numRows;
		this.rowGrpLen = rowGrpLen;
		this.numRowGroups = IntMath.divide(numRows, rowGrpLen, RoundingMode.CEILING);
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

		try
		{
			resetRandom();
			readAndAssertValuesStandardDownload(downloaded, info);
		}
		finally
		{
			Files.deleteIfExists(downloaded);
		}


		S3KeyDownloader keyDownloader = new S3KeyDownloader(s3MockService.getS3Client(), s3MockService.testBucket(),
				KEY);

		readAndAssertValuesDownloadToRowGroups(keyDownloader, new long[] { 0, keyDownloader.getObjectSize() });

		readAndAssertValuesDownloadToRowGroups(keyDownloader,
				new long[] { 0, keyDownloader.getObjectSize() / 3, keyDownloader.getObjectSize() });

		RowGroup firstRowGrp = info.getFileMetaData().getRow_groups().get(0);
		long endOffsetOfFirstRowGrp = firstRowGrp.getFile_offset() + firstRowGrp.getTotal_compressed_size();
		readAndAssertValuesDownloadToRowGroups(keyDownloader,
				new long[] { 0, endOffsetOfFirstRowGrp, keyDownloader.getObjectSize() });
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

	private void readAndAssertValuesStandardDownload(Path downloaded, ParquetFileInfo info) throws IOException
	{
		ParquetColumnarFileReader reader = new ParquetColumnarFileReader(downloaded);
		Assert.assertEquals(info.getFileMetaData().getRow_groupsSize(), reader.readMetaData().getRow_groupsSize());

		// this assumes the implementation of process file is single and in order. Seems not great.
		reader.processFile((ParquetColumnarProcessors.RowGroupProcessor) rowGroup -> {
			assertNextValuesEqual(rowGroup);
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

	private void readAndAssertValuesDownloadToRowGroups(S3KeyDownloader keyDownloader, long[] offsets)
			throws IOException
	{
		resetRandom();
		S3ParquetFilePartDownloader parquetFilePartDownloader = partDownloader(keyDownloader, offsets);
		Path tmpFolder = Files.createTempDirectory("s3_explode_rowgrps");
		try
		{
			BiFunction<Integer, RowGroup, Path> rowGrpNumToFile = (rowGrpNum, rg) -> tmpFolder.resolve(
					"row_grp_" + rowGrpNum + ".parquet");
			parquetFilePartDownloader.downloadToRowGroups(rowGrpNumToFile);

			for (int rgNum = 0; rgNum < numRowGroups; rgNum++)
			{
				Path rgPath = rowGrpNumToFile.apply(rgNum, null);
				Assert.assertTrue(Files.isRegularFile(rgPath));

				ParquetColumnarFileReader reader = new ParquetColumnarFileReader(rgPath);
				Assert.assertEquals(1, reader.readMetaData().getRow_groupsSize());

				reader.processFile((ParquetColumnarProcessors.RowGroupProcessor) rowGroup -> {
					assertNextValuesEqual(rowGroup);
				});
			}
		}
		finally
		{
			PathUtils.deleteDirectory(tmpFolder);
		}
	}

	private static S3ParquetFilePartDownloader partDownloader(S3KeyDownloader keyDownloader, long[] offsets)
	{
		S3ParquetFilePartDownloader parquetFilePartDownloader = new S3ParquetFilePartDownloader(keyDownloader)
		{
			@Override
			protected long[] computePartOffsets()
			{
				return offsets;
			}
		};
		return parquetFilePartDownloader;
	}

	private void assertNextValuesEqual(InMemRowGroup rowGroup)
	{
		Assert.assertTrue(rowGroup.getNumRows() <= rowGrpLen);
		rowGroup.forEachColumnChunk(inMemChunk -> {
			ChunkValuesReader columnReader = ChunkValuesReaderFactory.createChunkReader(inMemChunk);
			double[] vals = getRandomVals(inMemChunk.getDescriptor().getPath()[0], (int) inMemChunk.getTotalValues());
			for (int i = 0; i < rowGroup.getNumRows(); i++)
			{
				Assert.assertEquals(vals[i], columnReader.getDouble(), 0d);
				columnReader.next();
			}
		});
	}
}
