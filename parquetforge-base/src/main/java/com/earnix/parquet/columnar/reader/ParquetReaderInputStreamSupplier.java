package com.earnix.parquet.columnar.reader;

import org.apache.parquet.format.FileMetaData;

import java.io.IOException;
import java.io.InputStream;

public interface ParquetReaderInputStreamSupplier
{
	/**
	 * Read the metadata of the parquet file.
	 *
	 * @return the metadata of the parquet file
	 * @throws IOException on IO Failure
	 */
	FileMetaData readMetaData() throws IOException;

	/**
	 * Create a new {@link InputStream} for the start offset to read the specified number of bytes
	 *
	 * @param startOffset    the start offset of the input stream
	 * @param numBytesToRead the number of bytes to read
	 * @return the created {@link InputStream}. The caller MUST close the returned stream
	 * @throws IOException on failure accessing the parquet bytes
	 */
	InputStream createInputStream(long startOffset, long numBytesToRead) throws IOException;
}