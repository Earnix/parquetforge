package com.earnix.parquet.columnar.columnchunk;

public interface ColumnChunkWriter
{
	ColumnChunkPages writeColumn(String columnName, double[] vals);

	ColumnChunkPages writeColumn(String columnName, int[] vals);

	ColumnChunkPages writeColumn(String columnName, long[] vals);

	ColumnChunkPages writeColumn(String columnName, String[] vals);

	long totalBytesInRowGroup();
}
