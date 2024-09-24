package com.earnix.parquet.columnar.writer.columnchunk;

import java.util.Iterator;
import java.util.PrimitiveIterator;

/**
 * An interface to write parquet files by column.
 */
public interface ColumnChunkWriter
{
	ColumnChunkPages writeColumn(String columnName, double[] vals);

	ColumnChunkPages writeColumn(String columnName, PrimitiveIterator.OfDouble iterator);

	ColumnChunkPages writeColumn(String columnName, NullableIterators.NullableDoubleIterator iterator);

	ColumnChunkPages writeColumn(String columnName, int[] vals);

	ColumnChunkPages writeColumn(String columnName, PrimitiveIterator.OfInt iterator);

	ColumnChunkPages writeColumn(String columnName, NullableIterators.NullableIntegerIterator iterator);

	ColumnChunkPages writeColumn(String columnName, long[] vals);

	ColumnChunkPages writeColumn(String columnName, PrimitiveIterator.OfLong iterator);

	ColumnChunkPages writeColumn(String columnName, NullableIterators.NullableLongIterator iterator);

	ColumnChunkPages writeColumn(String columnName, boolean[] vals);

	ColumnChunkPages writeColumn(String columnName, Iterator<Boolean> iterator);

	ColumnChunkPages writeColumn(String columnName, String[] vals);

	ColumnChunkPages writeStringColumn(String columnName, Iterator<String> vals);
}
