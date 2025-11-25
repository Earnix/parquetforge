package com.earnix.parquet.columnar.file.reader;

import com.earnix.parquet.columnar.reader.IndexedParquetColumnarReader;
import com.earnix.parquet.columnar.reader.IndexedParquetColumnarReaderImpl;
import com.earnix.parquet.columnar.reader.ParquetReaderInputStreamSupplier;
import org.apache.parquet.format.FileMetaData;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

public class ParquetFileReaderFactory
{
	public static IndexedParquetColumnarReader createIndexedColumnarFileReader(Path parquetPath) throws IOException
	{
		return new IndexedParquetColumnarReaderImpl(new ParquetReaderInputStreamSupplier()
		{
			@Override
			public FileMetaData readMetaData() throws IOException
			{
				return ParquetFileMetadataReader.readFileMetadata(parquetPath);
			}

			@Override
			public InputStream createInputStream(long startOffset, long numBytesToRead) throws IOException
			{
				return new FileRangeInputStreamSupplier(parquetPath, startOffset, numBytesToRead).get();
			}
		});
	}
}
