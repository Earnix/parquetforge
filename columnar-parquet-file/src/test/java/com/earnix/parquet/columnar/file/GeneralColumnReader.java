package com.earnix.parquet.columnar.file;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.schema.PrimitiveType;

import java.nio.charset.StandardCharsets;

public class GeneralColumnReader
{
	public static Object getValue(ColumnReaderImpl reader, ColumnDescriptor columnDescriptor){
		reader.consume();

		if (valueIsNull(reader, columnDescriptor.getMaxDefinitionLevel()))
			return null;

		return getValueByType(reader, columnDescriptor.getPrimitiveType().getPrimitiveTypeName());
	}

	private static Object getValueByType(ColumnReaderImpl reader, PrimitiveType.PrimitiveTypeName primitiveTypeName)
	{
		switch (primitiveTypeName){
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

	private static boolean valueIsNull(ColumnReaderImpl reader, int defLevel)
	{
		return reader.getCurrentDefinitionLevel() < defLevel;
	}

}
