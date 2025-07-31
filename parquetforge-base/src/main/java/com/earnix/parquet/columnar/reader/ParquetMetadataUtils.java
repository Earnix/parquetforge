package com.earnix.parquet.columnar.reader;

import com.earnix.parquet.columnar.utils.ParquetEnumUtils;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Utils for processing parquet metadata
 */
public class ParquetMetadataUtils
{
	private static final String STRUCTURED_FILES_UNSUPPORTED = "Structured files are not yet supported";

	/**
	 * Build a message type schema from parquet footer metadata
	 *
	 * @param md the parquet footer metadata
	 * @return the message type
	 * @throws UnsupportedEncodingException if the schema is unsupported.
	 */
	public static MessageType buildMessageType(FileMetaData md) throws UnsupportedEncodingException
	{
		Iterator<SchemaElement> it = md.getSchemaIterator();
		SchemaElement root = it.next();
		if (root.getNum_children() + 1 != md.getSchemaSize())
		{
			throw new UnsupportedEncodingException(STRUCTURED_FILES_UNSUPPORTED);
		}

		List<Type> primitiveTypeList = new ArrayList<>(root.getNum_children());
		while (it.hasNext())
		{
			SchemaElement schemaElement = it.next();
			String nameKey = schemaElement.getName();
			if (schemaElement.getRepetition_type() != FieldRepetitionType.OPTIONAL
					&& schemaElement.getRepetition_type() != FieldRepetitionType.REQUIRED)
			{
				throw new UnsupportedEncodingException(
						"Field: " + nameKey + " Unsupported: " + schemaElement.getRepetition_type());
			}
			PrimitiveType.PrimitiveTypeName primitiveTypeName = ParquetEnumUtils.convert(schemaElement.getType());

			PrimitiveType primitiveType;
			if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
			{
				if (!schemaElement.isSetType_length() || schemaElement.getType_length() <= 0)
					throw new IllegalStateException("fixed length binary must have a valid len");
				primitiveType = new PrimitiveType(ParquetEnumUtils.convert(schemaElement.getRepetition_type()),
						primitiveTypeName, schemaElement.getType_length(), nameKey);
			}
			else
			{
				primitiveType = new PrimitiveType(ParquetEnumUtils.convert(schemaElement.getRepetition_type()),
						primitiveTypeName, nameKey);
			}

			primitiveTypeList.add(primitiveType);
		}
		return new MessageType(root.getName(), primitiveTypeList);
	}
}
