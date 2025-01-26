package com.earnix.parquet.columnar.file.writer;

import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

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
	 * @param outputFile  the file to write the parquet data to
	 * @param messageType the message schema
	 * @return the parquet file writer. Note that this MUST be closed externally.
	 * @throws IOException on failure to open the file
	 */
	public static ParquetColumnarWriter createWriter(Path outputFile, MessageType messageType,
			CompressionCodec compressionCodec, boolean cacheFileHandle) throws IOException
	{
		return new ParquetFileColumnarWriterImpl(outputFile, messageType, compressionCodec, cacheFileHandle);
	}

	/**
	 * Create a parquet file writer
	 *
	 * @param outputFile        the file to write the parquet data to
	 * @param primitiveTypeList the primitives
	 * @return the parquet file writer. Note that this MUST be closed externally.
	 * @throws IOException on failure to open the file
	 */
	public static ParquetColumnarWriter createWriter(Path outputFile, List<PrimitiveType> primitiveTypeList,
			boolean cacheFileHandle) throws IOException
	{
		return createWriter(outputFile, primitiveTypeList, CompressionCodec.ZSTD, cacheFileHandle);
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
			CompressionCodec compressionCodec, boolean cacheFileHandle) throws IOException
	{
		return createWriter(outputFile, new MessageType("root", primitiveTypeList.toArray(new Type[0])),
				compressionCodec, cacheFileHandle);
	}

	private ParquetFileColumnarWriterFactory()
	{
		// static methods ONLY
	}
}
