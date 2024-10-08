package com.earnix.parquet.columnar.utils;

import com.earnix.parquet.columnar.ColumnChunkForTesting;
import com.earnix.parquet.columnar.NullableLongIteratorImpl;
import com.earnix.parquet.columnar.writer.rowgroup.ChunkWriter;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.earnix.parquet.columnar.utils.Utils.convertExceptionToRuntime;

public class ColumnWritingUtil
{
	public static ColumnChunkForTesting writeDoubleColumn(RowGroupWriter groupWriter, String typeName, double[] vals)
	{
		return writeColumn(groupWriter, typeName,
				columnChunkWriter -> columnChunkWriter.writeColumn(typeName, vals),
				Arrays.stream(vals).boxed().collect(Collectors.toList()));
	}

	public static ColumnChunkForTesting writeBooleanColumn(RowGroupWriter groupWriter, String typeName, List<Boolean> vals)
	{
		return writeColumn(groupWriter, typeName,
				columnChunkWriter -> columnChunkWriter.writeColumn(typeName, vals.iterator()),
				vals);
	}

	public static ColumnChunkForTesting writeInt32Column(RowGroupWriter groupWriter, String typeName, int[] vals)
	{
		return writeColumn(groupWriter, typeName,
				columnChunkWriter -> columnChunkWriter.writeColumn(typeName, vals),
				Arrays.stream(vals).boxed().collect(Collectors.toList()));
	}

	public static ColumnChunkForTesting writeInt64Column(RowGroupWriter groupWriter, String typeName, NullableLongIteratorImpl nullableLongIterator)
	{
		return writeColumn(groupWriter, typeName,
				columnChunkWriter -> columnChunkWriter.writeColumn(typeName, nullableLongIterator),
				getLongValues(nullableLongIterator));
	}

	public static ColumnChunkForTesting writeBinaryColumn(RowGroupWriter groupWriter, String typeName, String[] vals)
	{
		return writeColumn(groupWriter, typeName,
				columnChunkWriter -> columnChunkWriter.writeColumn(typeName, vals),
				Arrays.asList(vals));
	}

	private static ColumnChunkForTesting writeColumn(RowGroupWriter groupWriter, String typeName, ChunkWriter chunkWriter, List<?> vals)
	{
		return convertExceptionToRuntime(() -> {
			groupWriter.writeColumn(chunkWriter);
			return new ColumnChunkForTesting(typeName, vals);
		});
	}

	private static List<Long> getLongValues(NullableLongIteratorImpl nullableLongIterator)
	{
		List<Long> longVals = new ArrayList<>();
		while (nullableLongIterator.next())
		{
			if (nullableLongIterator.isNull())
			{
				longVals.add(null);
			}
			else
			{
				longVals.add(nullableLongIterator.getValue());
			}
		}
		nullableLongIterator.reset();

		return longVals;
	}

}
