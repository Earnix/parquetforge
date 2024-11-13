package com.earnix.parquet.columnar.s3.assembler;

import org.apache.parquet.schema.MessageType;

import java.util.List;

public class S3ParquetAssembleAndUpload
{
	private final MessageType schema;

	public S3ParquetAssembleAndUpload(MessageType schema)
	{
		this.schema = schema;
	}

	public void assembleAndUpload(List<ParquetRowGroupSupplier> rowGroups)
	{

	}
}
