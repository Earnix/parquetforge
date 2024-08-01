package com.earnix.parquet.columnar.reader;

import org.apache.parquet.format.FileMetaData;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class ParquetColumarFileReader
{
	private final Path parquetFilePath;

	public ParquetColumarFileReader(Path parquetFilePath)
	{
		this.parquetFilePath = parquetFilePath;
	}

	public void readFile() throws IOException
	{
		try (FileChannel fc = FileChannel.open(parquetFilePath))
		{
			FileMetaData metaData = ParquetFileMetadataReader.readMetadata(fc);
		}
	}
}
