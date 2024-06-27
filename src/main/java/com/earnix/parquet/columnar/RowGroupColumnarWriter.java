package com.earnix.parquet.columnar;

public interface RowGroupColumnarWriter
{
	void writeColumn(String columnName, double[] vals);

	void writeColumn(String columnName, int[] vals);

	void writeColumn(String columnName, long[] vals);

	void writeColumn(String columnName, String[] vals);

	void finishGroup();
}
