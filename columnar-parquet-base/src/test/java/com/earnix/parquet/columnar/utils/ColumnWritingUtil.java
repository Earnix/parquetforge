package com.earnix.parquet.columnar.utils;

import com.earnix.parquet.columnar.NullableLongIteratorImpl;
import com.earnix.parquet.columnar.writer.rowgroup.ChunkValuesWritingFunction;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupWriter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ColumnWritingUtil
{
	public static ColumnChunkForTesting writeDoubleColumn(RowGroupWriter groupWriter, String typeName, double[] vals)
	{
		return writeColumn(groupWriter, typeName, columnChunkWriter -> columnChunkWriter.writeColumn(typeName, vals), Arrays.stream(vals).boxed().collect(Collectors.toList()));
	}

	public static ColumnChunkForTesting writeBooleanColumn(RowGroupWriter groupWriter, String typeName, List<Boolean> vals)
	{
		return writeColumn(groupWriter, typeName, columnChunkWriter -> columnChunkWriter.writeColumn(typeName, vals.iterator()), vals);
	}

	public static ColumnChunkForTesting writeBooleanColumn(RowGroupWriter groupWriter, String typeName, Iterator<Boolean> iterator, Iterator<Boolean> sameiIterator)
	{
		return writeColumn(groupWriter, typeName, columnChunkWriter -> columnChunkWriter.writeColumn(typeName, iterator), getBooleanValues(sameiIterator));
	}


	public static ColumnChunkForTesting writeInt32Column(RowGroupWriter groupWriter, String typeName, int[] vals)
	{
		return writeColumn(groupWriter, typeName, columnChunkWriter -> columnChunkWriter.writeColumn(typeName, vals), Arrays.stream(vals).boxed().collect(Collectors.toList()));
	}

	public static ColumnChunkForTesting writeInt64Column(RowGroupWriter groupWriter, String typeName, NullableLongIteratorImpl nullableLongIterator)
	{
		return writeColumn(groupWriter, typeName, columnChunkWriter -> columnChunkWriter.writeColumn(typeName, nullableLongIterator), getLongValues(nullableLongIterator));
	}

	public static ColumnChunkForTesting writeInt64Column(RowGroupWriter groupWriter, String typeName, long[] vals)
	{
		return writeColumn(groupWriter, typeName, columnChunkWriter -> columnChunkWriter.writeColumn(typeName, vals), Arrays.stream(vals).boxed().collect(Collectors.toList()));
	}

	public static ColumnChunkForTesting writeBinaryColumn(RowGroupWriter groupWriter, String typeName, String[] vals)
	{
		return writeColumn(groupWriter, typeName, columnChunkWriter -> columnChunkWriter.writeColumn(typeName, vals), Arrays.asList(vals));
	}

	public static ColumnChunkForTesting writeBinaryColumn(RowGroupWriter groupWriter, String typeName, Iterator<String> vals, Iterator<String> identicalVals)
	{
		return writeColumn(groupWriter, typeName, columnChunkWriter -> columnChunkWriter.writeStringColumn(typeName, vals), toList(identicalVals));
	}


	private static ColumnChunkForTesting writeColumn(RowGroupWriter groupWriter, String typeName, ChunkValuesWritingFunction chunkValuesWritingFunction, List<?> vals)
	{
		try
		{
			groupWriter.writeValues(chunkValuesWritingFunction);
			return new ColumnChunkForTesting(typeName, vals);
		}
		catch (IOException ex)
		{
			throw new UncheckedIOException(ex);
		}
	}

	private static List<Long> getLongValues(NullableLongIteratorImpl nullableLongIterator)
	{
		List<Long> longVals = new ArrayList<>();
		while (nullableLongIterator.next())
		{
			longVals.add(nullableLongIterator.isNull() ? null : nullableLongIterator.getValue());
		}
		nullableLongIterator.reset();

		return longVals;
	}

	private static List<Boolean> getBooleanValues(Iterator<Boolean> iterator)
	{
		List<Boolean> boolVals = new ArrayList<>();
		iterator.forEachRemaining(boolVals::add);
		return boolVals;
	}

	private static List<String> toList(Iterator<String> iterator)
	{
		List<String> list = new ArrayList<>();
		iterator.forEachRemaining(list::add);
		return list;
	}

}
