package com.earnix.parquet.columnar.file;

import com.earnix.parquet.columnar.NullableLongIteratorImpl;
import com.earnix.parquet.columnar.file.writer.ParquetFileColumnarWriterFactory;
import com.earnix.parquet.columnar.utils.ColumnChunkForTesting;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupWriter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.earnix.parquet.columnar.utils.ColumnWritingUtil.writeBinaryColumn;
import static com.earnix.parquet.columnar.utils.ColumnWritingUtil.writeBooleanColumn;
import static com.earnix.parquet.columnar.utils.ColumnWritingUtil.writeDoubleColumn;
import static com.earnix.parquet.columnar.utils.ColumnWritingUtil.writeInt32Column;
import static com.earnix.parquet.columnar.utils.ColumnWritingUtil.writeInt64Column;

public class ParquetFileFiller
{

	public static List<RowGroupForTesting> fillWithRowGroups(Path parquetFile)
	{
		try (ParquetColumnarWriter rowGroupWriter = ParquetFileColumnarWriterFactory.createWriter(parquetFile,
				PARQUET_COLUMNS, false))
		{
			List<RowGroupForTesting> rowGroups = new ArrayList<>();
			rowGroups.add(writeRowGroupWith2Rows(rowGroupWriter));
			rowGroups.add(writeRowGroupWith1Row(rowGroupWriter));
			rowGroups.add(writeRowGroupWithLotOfRows(rowGroupWriter));

			rowGroupWriter.finishAndWriteFooterMetadata();
			return rowGroups;
		}
		catch (IOException ex)
		{
			throw new UncheckedIOException(ex);
		}
	}

	private static final String COL_1_DOUBLE_NAME = "COL_1_DOUBLE";
	private static final String COL_BOOLEAN_2_NAME = "COL_2_BOOLEAN";
	private static final String COL_3_INT_32_NAME = "COL_3_INT32";
	private static final String COL_4_INT_64_NAME = "COL_4_INT64";
	private static final String COL_5_BINARY_NAME = "COL_5_BINARY";
	private static final List<PrimitiveType> PARQUET_COLUMNS = Arrays.asList(
			new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, COL_1_DOUBLE_NAME),
			new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BOOLEAN, COL_BOOLEAN_2_NAME),
			new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, COL_3_INT_32_NAME),
			new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.INT64, COL_4_INT_64_NAME),
			new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, COL_5_BINARY_NAME));

	// List is copied so generics is happy.
	private static final MessageType messageType = new MessageType("root", new ArrayList<>(PARQUET_COLUMNS));
	private static final ColumnDescriptor COL_1_DOUBLE = messageType.getColumnDescription(
			new String[] { COL_1_DOUBLE_NAME });
	private static final ColumnDescriptor COL_BOOLEAN_2 = messageType.getColumnDescription(
			new String[] { COL_BOOLEAN_2_NAME });
	private static final ColumnDescriptor COL_3_INT_32 = messageType.getColumnDescription(
			new String[] { COL_3_INT_32_NAME });
	private static final ColumnDescriptor COL_4_INT_64 = messageType.getColumnDescription(
			new String[] { COL_4_INT_64_NAME });
	private static final ColumnDescriptor COL_5_BINARY = messageType.getColumnDescription(
			new String[] { COL_5_BINARY_NAME });

	private static RowGroupForTesting writeRowGroupWith2Rows(ParquetColumnarWriter parquetColumnarWriter)
			throws IOException
	{
		List<Function<RowGroupWriter, ColumnChunkForTesting>> chunkBuilders = Arrays.asList(
				writer -> writeDoubleColumn(writer, COL_1_DOUBLE, new double[] { 1.3, 2.4 }),
				writer -> writeBooleanColumn(writer, COL_BOOLEAN_2, Arrays.asList(false, null)),
				writer -> writeInt32Column(writer, COL_3_INT_32, new int[] { 4, 6 }),
				writer -> writeInt64Column(writer, COL_4_INT_64, new NullableLongIteratorImpl()),
				writer -> writeBinaryColumn(writer, COL_5_BINARY, new String[] { "burrito", "taco" }));

		return writeRowGroupFromBuilders(2, chunkBuilders, parquetColumnarWriter);
	}

	private static RowGroupForTesting writeRowGroupWith1Row(ParquetColumnarWriter parquetColumnarWriter)
			throws IOException
	{
		List<Function<RowGroupWriter, ColumnChunkForTesting>> chunkBuilders = Arrays.asList(
				writer -> writeDoubleColumn(writer, COL_1_DOUBLE, new double[] { 30.6 }),
				writer -> writeBooleanColumn(writer, COL_BOOLEAN_2, Arrays.asList(Boolean.FALSE)),
				writer -> writeInt32Column(writer, COL_3_INT_32, new int[] { 4 }),
				writer -> writeInt64Column(writer, COL_4_INT_64, new long[] { 4 }),
				writer -> writeBinaryColumn(writer, COL_5_BINARY, new String[] { "cheezburger" }));

		return writeRowGroupFromBuilders(1, chunkBuilders, parquetColumnarWriter);
	}

	private static RowGroupForTesting writeRowGroupWithLotOfRows(ParquetColumnarWriter parquetColumnarWriter)
			throws IOException
	{
		int LOTS_OF_ROWS = 10_000;

		List<Function<RowGroupWriter, ColumnChunkForTesting>> chunkBuilders = Arrays.asList(
				writer -> writeDoubleColumn(writer, COL_1_DOUBLE,
						IntStream.range(0, LOTS_OF_ROWS).mapToDouble(Double::valueOf).toArray()),
				writer -> writeBooleanColumn(writer, COL_BOOLEAN_2,
						IntStream.range(0, LOTS_OF_ROWS).mapToObj(i -> i % 2 == 0).iterator(),
						IntStream.range(0, LOTS_OF_ROWS).mapToObj(i -> i % 2 == 0).iterator()),
				writer -> writeInt32Column(writer, COL_3_INT_32, IntStream.range(0, LOTS_OF_ROWS).toArray()),
				writer -> writeInt64Column(writer, COL_4_INT_64, LongStream.range(0, LOTS_OF_ROWS).toArray()),
				writer -> writeBinaryColumn(writer, COL_5_BINARY,
						IntStream.range(0, LOTS_OF_ROWS).mapToObj(i -> "Cheeseburger" + i % 10).iterator(),
						IntStream.range(0, LOTS_OF_ROWS).mapToObj(i -> "Cheeseburger" + i % 10).iterator()));

		return writeRowGroupFromBuilders(LOTS_OF_ROWS, chunkBuilders, parquetColumnarWriter);
	}

	private static RowGroupForTesting writeRowGroupFromBuilders(int rowsNumber,
			List<Function<RowGroupWriter, ColumnChunkForTesting>> chunkBuilders,
			ParquetColumnarWriter parquetColumnarWriter) throws IOException
	{
		RowGroupForTesting expectedRowGroup = new RowGroupForTesting(rowsNumber);
		parquetColumnarWriter.writeRowGroup(rowsNumber,
				groupWriter -> chunkBuilders.forEach(builder -> expectedRowGroup.addChunk(builder.apply(groupWriter))));
		return expectedRowGroup;
	}

}
