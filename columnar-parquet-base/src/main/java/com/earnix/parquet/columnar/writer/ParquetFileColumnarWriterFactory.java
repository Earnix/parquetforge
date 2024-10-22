package com.earnix.parquet.columnar.writer;

import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class ParquetFileColumnarWriterFactory
{
	public static ParquetColumnarWriter createWriter(Path outputFile, List<PrimitiveType> primitiveTypeList,
			CompressionCodec compressionCodec) throws IOException
	{
		return new ParquetFileColumnarWriterImpl(outputFile, primitiveTypeList, compressionCodec);
	}
}
