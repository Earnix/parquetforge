package com.earnix.parquet.columnar.file;

import com.earnix.parquet.columnar.file.reader.ParquetFileReaderFactory;
import com.earnix.parquet.columnar.file.writer.ParquetFileColumnarWriterFactory;
import com.earnix.parquet.columnar.reader.IndexedParquetColumnarReader;
import com.earnix.parquet.columnar.utils.ParquetMagicUtils;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import static java.util.Arrays.asList;

public class DictionaryWritingTest
{
	/**
	 * Tests writing a file with a column that will be a dictionary and another column that will not be a dictionary
	 * making sure that dict offset in the metadata gets set correctly.
	 */
	@Test
	public void testWriteDict() throws IOException
	{

		String notDict = "NotDict";
		String yesDict = "YesDict";
		List<Type> parquetCols = asList(
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, notDict),
				new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT32, yesDict));

		MessageType messageType = new MessageType("root", parquetCols);
		Path tmpFile = Files.createTempFile("testDict", ".parquet");
		try
		{
			writeParquet(tmpFile, messageType, notDict, yesDict);

			IndexedParquetColumnarReader reader = ParquetFileReaderFactory.createIndexedColumnarFileReader(tmpFile);
			Assert.assertEquals(1, reader.getNumRowGroups());
			ColumnChunk cc1 = reader.getColumnChunk(0, reader.getDescriptor(0));
			Assert.assertEquals(asList(notDict), cc1.getMeta_data().getPath_in_schema());
			Assert.assertFalse(cc1.getMeta_data().isSetDictionary_page_offset());
			Assert.assertTrue(cc1.getMeta_data().getData_page_offset() >= ParquetMagicUtils.PARQUET_MAGIC.length());

			ColumnChunk cc2 = reader.getColumnChunk(0, reader.getDescriptor(1));
			Assert.assertEquals(asList(yesDict), cc2.getMeta_data().getPath_in_schema());
			Assert.assertTrue(cc2.getMeta_data().isSetDictionary_page_offset());
			Assert.assertTrue("Dict page offset must be set, and after the parquet magic",
					cc2.getMeta_data().getDictionary_page_offset() >= ParquetMagicUtils.PARQUET_MAGIC.length());
			Assert.assertTrue("Data page must be set and after the dictionary.",
					cc2.getMeta_data().getDictionary_page_offset() < cc2.getMeta_data().getData_page_offset());
		}
		finally
		{
			Files.deleteIfExists(tmpFile);
		}
	}

	private static void writeParquet(Path tmpFile, MessageType messageType, String notDict, String yesDict)
			throws IOException
	{
		try (ParquetColumnarWriter rowGroupWriter = ParquetFileColumnarWriterFactory.createWriter(tmpFile, messageType,
				CompressionCodec.ZSTD, false))
		{
			int numRows = 10_000;
			rowGroupWriter.writeRowGroup(numRows, rgw -> {
				rgw.writeValues(cvw -> cvw.writeColumn(messageType.getColumnDescription(new String[] { notDict }),
						makeRandCol(numRows)));
				rgw.writeValues(cvw -> cvw.writeColumn(messageType.getColumnDescription(new String[] { yesDict }),
						makeDictCol(numRows)));
			});

			rowGroupWriter.finishAndWriteFooterMetadata();
		}
	}

	private static int[] makeRandCol(int numElements)
	{
		Random r = new Random(numElements);
		return r.ints(numElements).toArray();
	}

	private static int[] makeDictCol(int numElements)
	{
		if (numElements < 1000)
			throw new IllegalArgumentException("Probably too few elements for dict col");

		Random r = new Random(numElements);
		int dictSize = numElements / 100;
		int[] dict = r.ints(dictSize).toArray();

		int[] ret = new int[numElements];
		for (int i = 0; i < ret.length; i++)
		{
			ret[i] = dict[r.nextInt(dict.length)];
		}
		return ret;
	}
}
