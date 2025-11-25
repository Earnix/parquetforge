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

	InputStream createInputStream(long startOffset, long numBytesToRead) throws IOException;
}