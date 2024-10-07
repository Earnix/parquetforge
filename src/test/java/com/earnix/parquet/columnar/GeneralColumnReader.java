package com.earnix.parquet.columnar;

import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import java.nio.charset.StandardCharsets;

public class GeneralColumnReader
{
	public static <T> T getValue(ColumnReaderImpl reader, PrimitiveType.PrimitiveTypeName primitiveTypeName,
			int defLevel, Class<T> objectClass){
		reader.consume();
		if(reader.getCurrentDefinitionLevel() < defLevel)
			return null;
		Object value;
//		switch (primitiveTypeName){
//			case INT64:
//				value = reader.getLong();
//				break;
//		}
		if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.INT64)){
			value = reader.getLong();
		}
		else if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.INT32)){
			value = reader.getInteger();
		}
		else if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.BOOLEAN)){
			value= reader.getBoolean();
		}
		else if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.BINARY)){
			byte[] binaryBytes = reader.getBinary().getBytes();
			value = new String(binaryBytes, StandardCharsets.UTF_8);
		}
		else if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)){
			value = reader.getBinary();
		}
		else if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.FLOAT)){
			value = reader.getFloat();
		}
		else if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.DOUBLE)){
			value = reader.getDouble();
		}
		else {
			throw new IllegalStateException("Invalid Type " + primitiveTypeName);
		}

		System.out.println("val=" + value);
		if (value == null)
		{
			return null;
		}
		else
		{
			return objectClass.cast(value);
		}
	}

	public static Class<?> convertToObjectClass(PrimitiveType.PrimitiveTypeName primitiveTypeName){
		if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.INT64)){
			return Long.class;
		}
		else if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.INT32)){
			return Integer.class;
		}
		else if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.BOOLEAN)){
			return Boolean.class;
		}
		else if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.BINARY)){
			return String.class;
		}
		else if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)){
			return Binary.class;
		}
		else if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.FLOAT)){
			return Float.class;
		}
		else if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.DOUBLE)){
			return Double.class;
		}
		else {
			throw new IllegalStateException("Invalid Type " + primitiveTypeName);
		}
	}

}
