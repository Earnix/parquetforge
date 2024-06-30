package com.earnix.parquet.columnar.config;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;

public class ParquetWriteConfig
{
	private final ParquetProperties parquetProperties;
	private final CompressionCodec compressionCodec;
	private final int zstdCompressionLevel;

	public ParquetWriteConfig()
	{
		this(ParquetProperties.builder().build(), CompressionCodec.ZSTD, 2);
	}

	public ParquetWriteConfig(ParquetProperties parquetProperties, CompressionCodec compressionCodec,
			int zstdCompressionLevel)
	{
		this.parquetProperties = parquetProperties;
		this.compressionCodec = compressionCodec;
		this.zstdCompressionLevel = zstdCompressionLevel;
	}

	public ParquetProperties getParquetProperties()
	{
		return parquetProperties;
	}

	public CompressionCodec getCompressionCodec()
	{
		return compressionCodec;
	}

	public int getZstdCompressionLevel()
	{
		return zstdCompressionLevel;
	}
}
