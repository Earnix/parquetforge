package com.earnix.parquet.columnar.reader;

import com.earnix.parquet.columnar.utils.ParquetMetadataConverterUtils;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.earnix.parquet.columnar.utils.ParquetEnumUtils.convert;
import static java.util.Objects.requireNonNullElse;

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
			PrimitiveType.PrimitiveTypeName primitiveTypeName = convert(schemaElement.getType());
			PrimitiveType primitiveType;
			if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
			{
				if (!schemaElement.isSetType_length() || schemaElement.getType_length() <= 0)
					throw new IllegalStateException("fixed length binary must have a valid len");
				primitiveType = new PrimitiveType(convert(schemaElement.getRepetition_type()),
						primitiveTypeName, schemaElement.getType_length(), nameKey);
			}
			else
			{
				primitiveType = new PrimitiveType(convert(schemaElement.getRepetition_type()),
						primitiveTypeName, nameKey);
			}

			if (schemaElement.isSetLogicalType())
			{
				// todo - logical type conversion
				primitiveType = primitiveType.withLogicalTypeAnnotation(
						ParquetMetadataConverterUtils.getLogicalTypeAnnotation(schemaElement.getLogicalType()));
			}
			else if (schemaElement.isSetConverted_type())
			{
				// deprecated old convertedType
				int scale = (schemaElement.isSetScale() ? 0 : schemaElement.scale);
				int precision = (schemaElement.isSetPrecision() ? 0 : schemaElement.precision);
				DecimalMetadata decimalMetadata = new DecimalMetadata(scale, precision);
				primitiveType = primitiveType.withLogicalTypeAnnotation(
						LogicalTypeAnnotation.fromOriginalType(convert(schemaElement.getConverted_type()),
								decimalMetadata));
			}

			primitiveTypeList.add(primitiveType);
		}
		return new MessageType(root.getName(), primitiveTypeList);
	}

	public static List<KeyValue> deepCopyKeyValueMetadata(List<KeyValue> keyValuesMetadata)
	{
		return requireNonNullElse(keyValuesMetadata, List.<KeyValue> of()).stream()//
				.map(KeyValue::new)//
				.collect(Collectors.toUnmodifiableList());
	}
}
