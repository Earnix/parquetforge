package com.earnix.parquet.columnar.utils;

import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.schema.OriginalType;
import org.junit.Assert;
import org.junit.Test;

public class ParquetEnumUtilsTest
{
	@Test
	public void testConvertedType()
	{
		for (ConvertedType ct : ConvertedType.values())
		{
			Assert.assertEquals(ct.toString(), ParquetEnumUtils.convert(ct).toString());
		}
		for (OriginalType ct : OriginalType.values())
		{
			Assert.assertEquals(ct.toString(), ParquetEnumUtils.convert(ct).toString());
		}
	}
}
