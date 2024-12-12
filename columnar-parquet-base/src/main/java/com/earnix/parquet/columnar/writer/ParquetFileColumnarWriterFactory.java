package com.earnix.parquet.columnar.writer;

import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * A factory to create parquet column file writers
 */
public class ParquetFileColumnarWriterFactory
{
	/**
	 * Create a parquet file writer
	 *
	 * @param outputFile        the file to write the parquet data to
	 * @param primitiveTypeList the primitives
	 * @return the parquet file writer. Note that this MUST be closed externally.
	 * @throws IOException on failure to open the file
	 */
	public static ParquetColumnarWriter createWriter(Path outputFile, List<PrimitiveType> primitiveTypeList)
			throws IOException
	{
		return new ParquetFileColumnarWriterImpl(outputFile, primitiveTypeList, CompressionCodec.ZSTD);
	}

	/**
	 * Create a parquet file writer
	 *
	 * @param outputFile        the file to write the parquet data to
	 * @param primitiveTypeList the primitives
	 * @param compressionCodec  the compression codec to use
	 * @return the parquet file writer. Note that this MUST be closed externally.
	 * @throws IOException on failure to open the file
	 */
	public static ParquetColumnarWriter createWriter(Path outputFile, List<PrimitiveType> primitiveTypeList,
			CompressionCodec compressionCodec) throws IOException
	{
		return new ParquetFileColumnarWriterImpl(outputFile, primitiveTypeList, compressionCodec);
	}

	private ParquetFileColumnarWriterFactory()
	{
		// static methods ONLY
	}
}
