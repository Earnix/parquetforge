package com.earnix.parquet.columnar.utils;

import org.apache.parquet.format.Encoding;

public class ParquetEnumUtils
{

	public static Encoding convert(org.apache.parquet.column.Encoding encoding)
	{
		return Encoding.valueOf(encoding.name());
	}

	public static org.apache.parquet.column.Encoding convert(Encoding encoding)
	{
		return org.apache.parquet.column.Encoding.valueOf(encoding.name());
	}
}
