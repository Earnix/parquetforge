package com.earnix.parquet.columnar;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.List;

import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public class ParquetFileColumnarWriterImpl implements ParquetFileColumnarWriter
{
	private final MessageType messageType;
	private final ParquetProperties parquetProperties;

	/**
	 * Constructor for flat file.
	 * 
	 * @param primitiveTypeList the types of columns
	 */
	public ParquetFileColumnarWriterImpl(List<PrimitiveType> primitiveTypeList)
	{
		for (PrimitiveType type : primitiveTypeList)
		{
			if (type.getRepetition() != REQUIRED)
				throw new IllegalStateException("Only required is supported for now");
		}
		messageType = new MessageType("root", primitiveTypeList.toArray(new Type[0]));

		// this probably should be a constructor param
		parquetProperties = ParquetProperties.builder().build();
	}

	@Override
	public RowGroupColumnarWriter startNewRowGroup(int numRows)
	{
		return new RowGroupColumnarWriterImpl(messageType, parquetProperties, numRows);
	}

	@Override
	public void finishAndWriteFooterMetadata()
	{

	}

	@Override
	public void close() throws IOException
	{

	}
}
