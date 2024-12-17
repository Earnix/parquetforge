package com.earnix.parquet.columnar.file;

import com.earnix.parquet.columnar.reader.chunk.ChunkValuesReader;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.PrimitiveType;

import java.nio.charset.StandardCharsets;

public class GeneralColumnReader
{
	public static Object getValue(ChunkValuesReader reader, ColumnDescriptor columnDescriptor)
	{
		if (reader.isNull())
			return null;

		return getValueByType(reader, columnDescriptor.getPrimitiveType().getPrimitiveTypeName());
	}

	private static Object getValueByType(ChunkValuesReader reader, PrimitiveType.PrimitiveTypeName primitiveTypeName)
	{
		switch (primitiveTypeName)
		{
			case INT64:
				return reader.getLong();
			case INT32:
				return reader.getInteger();
			case BOOLEAN:
				return reader.getBoolean();
			case BINARY:
				byte[] binaryBytes = reader.getBinary().getBytes();
				return new String(binaryBytes, StandardCharsets.UTF_8);
			case FIXED_LEN_BYTE_ARRAY:
				return reader.getBinary();
			case FLOAT:
				return reader.getFloat();
			case DOUBLE:
				return reader.getDouble();
			default:
				throw new IllegalStateException("Unknown Type " + primitiveTypeName);
		}
	}
}
