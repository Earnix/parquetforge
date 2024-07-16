package com.earnix.parquet.columnar.columnchunk;

import java.util.Iterator;
import java.util.PrimitiveIterator;

public interface ColumnChunkWriter
{
	ColumnChunkPages writeColumn(String columnName, double[] vals);

	ColumnChunkPages writeColumn(String columnName, PrimitiveIterator.OfDouble iterator);

	ColumnChunkPages writeColumn(String columnName, int[] vals);

	ColumnChunkPages writeColumn(String columnName, PrimitiveIterator.OfInt iterator);

	ColumnChunkPages writeColumn(String columnName, long[] vals);

	ColumnChunkPages writeColumn(String columnName, PrimitiveIterator.OfLong iterator);
	ColumnChunkPages writeColumn(String columnName, NullableIterators.NullableLongIterator iterator);

	ColumnChunkPages writeColumn(String columnName, String[] vals);

	ColumnChunkPages writeColumn(String columnName, Iterator<String> vals);

	long totalBytesInRowGroup();
}
