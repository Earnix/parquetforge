package com.earnix.parquet.columnar.file;

import com.earnix.parquet.columnar.file.reader.ParquetFileMetadataReader;
import com.earnix.parquet.columnar.file.reader.ParquetFileReaderFactory;
import com.earnix.parquet.columnar.file.writer.ParquetFileColumnarWriterFactory;
import com.earnix.parquet.columnar.reader.IndexedParquetColumnarReader;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupWriter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
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

import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.float16Type;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

/**
 * Test logical type and original type is written and read correctly.
 */
public class LogicalTypeReadWriteTest
{
	private static final List<PrimitiveType> PARQUET_COLUMNS = Arrays.asList(//
			Types.optional(FLOAT).named("float"),
			Types.optional(FIXED_LEN_BYTE_ARRAY).length(2).named("float16").withLogicalTypeAnnotation(float16Type()),
			Types.optional(DOUBLE).named("double"), Types.optional(INT32).named("int"),
			Types.optional(INT32).named("int32").withLogicalTypeAnnotation(intType(32)),
			Types.optional(INT32).named("int16").withLogicalTypeAnnotation(intType(16)),
			Types.optional(INT32).named("uint16").withLogicalTypeAnnotation(intType(16, false)),
			Types.optional(INT64).named("long"),
			Types.optional(INT64).named("long64").withLogicalTypeAnnotation(intType(64)),
			Types.optional(INT64).named("long32").withLogicalTypeAnnotation(intType(32)),
			Types.optional(INT64).named("ulong32").withLogicalTypeAnnotation(intType(32, false)),
			Types.optional(BINARY).named("binary"),
			Types.optional(BINARY).named("string").withLogicalTypeAnnotation(stringType()),
			Types.optional(BINARY).named("enum").withLogicalTypeAnnotation(enumType()),
			Types.optional(FIXED_LEN_BYTE_ARRAY).length(2).named("fixed"));

	// List is copied so generics is happy.
	private static final MessageType messageType = new MessageType("root", new ArrayList<>(PARQUET_COLUMNS));

	private Path parquetFile;

	@Before
	public void setUp() throws Exception
	{
		parquetFile = Files.createTempFile("parquetfile", ".parquet");
	}

	@After
	public void tearDown() throws Exception
	{
		Files.deleteIfExists(parquetFile);
	}

	@Test
	public void lotsOfLogicalTypes() throws Exception
	{
		try (ParquetColumnarWriter rowGroupWriter = ParquetFileColumnarWriterFactory.createWriter(parquetFile,
				messageType, CompressionCodec.ZSTD, true))
		{
			rowGroupWriter.writeRowGroup(1, this::writeCols);
			rowGroupWriter.finishAndWriteFooterMetadata();
		}

		// assert that logical types are set where expected
		FileMetaData metadata = ParquetFileMetadataReader.readFileMetadata(parquetFile);
		Assert.assertEquals(PARQUET_COLUMNS.size(), metadata.getSchemaSize() - 1);
		for (int i = 0; i < PARQUET_COLUMNS.size(); ++i)
		{
			SchemaElement schemaElement = metadata.getSchema().get(i + 1);
			if (PARQUET_COLUMNS.get(i).getLogicalTypeAnnotation() != null)
			{
				Assert.assertNotNull(schemaElement.getLogicalType());
			}
			else
			{
				Assert.assertNull(schemaElement.getLogicalType());
			}
		}


		IndexedParquetColumnarReader reader = ParquetFileReaderFactory.createIndexedColumnarFileReader(parquetFile);
		MessageType readMessageType = reader.getMessageType();
		Assert.assertEquals(messageType, readMessageType);
		Assert.assertEquals(messageType.getColumns(), readMessageType.getColumns());
		Assert.assertArrayEquals(messageType.getColumns().stream().map(ColumnDescriptor::getPrimitiveType).toArray(),
				readMessageType.getColumns().stream().map(ColumnDescriptor::getPrimitiveType).toArray());
	}

	private void writeCols(RowGroupWriter writer) throws IOException
	{
		for (ColumnDescriptor cd : messageType.getColumns())
		{
			switch (cd.getPrimitiveType().getPrimitiveTypeName())
			{
				case DOUBLE:
					writer.writeValues(w -> w.writeColumn(cd, new double[] { 3 }));
					break;
				case INT32:
					writer.writeValues(w -> w.writeColumn(cd, new int[] { 3 }));
					break;
				case INT64:
					writer.writeValues(w -> w.writeColumn(cd, new long[] { 3 }));
					break;
				case FLOAT:
					writer.writeValues(w -> w.writeColumn(cd, new float[] { 3 }));
					break;
				case FIXED_LEN_BYTE_ARRAY:
					writer.writeValues(w -> w.writeColumn(cd, new String[] { "12" }));
					break;
				case BINARY:
					writer.writeValues(w -> w.writeColumn(cd, new String[] { "1234" }));
					break;
				default:
					Assert.fail("unknown " + cd.getPrimitiveType());
			}
		}
	}
}
