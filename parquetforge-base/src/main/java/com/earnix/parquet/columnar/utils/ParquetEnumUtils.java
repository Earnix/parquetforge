package com.earnix.parquet.columnar.utils;

import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

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

	public static org.apache.parquet.format.Type convert(PrimitiveType.PrimitiveTypeName primitiveTypeName)
	{
		if (PrimitiveType.PrimitiveTypeName.BINARY.equals(primitiveTypeName))
		{
			return org.apache.parquet.format.Type.BYTE_ARRAY;
		}
		return org.apache.parquet.format.Type.valueOf(primitiveTypeName.name());
	}

	public static PrimitiveType.PrimitiveTypeName convert(org.apache.parquet.format.Type primitiveTypeName)
	{
		if (org.apache.parquet.format.Type.BYTE_ARRAY.equals(primitiveTypeName))
		{
			return PrimitiveType.PrimitiveTypeName.BINARY;
		}
		return PrimitiveType.PrimitiveTypeName.valueOf(primitiveTypeName.name());
	}


	public static FieldRepetitionType convert(Type.Repetition repetition)
	{
		return FieldRepetitionType.valueOf(repetition.name());
	}

	public static Type.Repetition convert(FieldRepetitionType repetition)
	{
		return Type.Repetition.valueOf(repetition.name());
	}

	public static ConvertedType convert(OriginalType originalType)
	{
		return ConvertedType.valueOf(originalType.toString());
	}

	public static OriginalType convert(ConvertedType convertedType)
	{
		return OriginalType.valueOf(convertedType.toString());
	}
}
