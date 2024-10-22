package com.earnix.parquet.columnar.writer.compressors;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;

import com.github.luben.zstd.Zstd;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.Arrays;

public class CompressorZstdImpl implements CompressionCodecFactory.BytesInputCompressor,
		com.earnix.parquet.columnar.writer.compressors.Compressor
{
	public static final String COMPRESSION_LEVEL_PROPERTY = "com.earnix.datatable.compression.zstdlevel";

	@Override
	public int compress(byte[] input, byte[] output)
	{
		long compressedSize = Zstd.compress(output, input, getCompressionLevel());
		if (Zstd.isError(compressedSize))
		{
			throw new IllegalStateException("Error compressing bytes: " + compressedSize);
		}
		return Math.toIntExact(compressedSize);
	}

	private static int getCompressionLevel()
	{
		int compressionLevel = Integer.parseInt(System.getProperty(COMPRESSION_LEVEL_PROPERTY, "2"));
		return compressionLevel;
	}

	@Override
	public int maxCompressedLength(int numBytes)
	{
		long bound = Zstd.compressBound(numBytes);
		return Math.toIntExact(bound);
	}

	@Override
	public BytesInput compress(BytesInput bytes) throws IOException
	{
		byte[] compressedBytes = new byte[maxCompressedLength(Math.toIntExact(bytes.size()))];
		int size = compress(bytes.toByteArray(), compressedBytes);

		// if much smaller, copy/shrink
		if (size < compressedBytes.length / 2)
			compressedBytes = Arrays.copyOf(compressedBytes, size);

		return BytesInput.from(compressedBytes, 0, size);
	}

	@Override
	public CompressionCodecName getCodecName()
	{
		return CompressionCodecName.ZSTD;
	}

	@Override
	public void release()
	{
		// do nothing.
	}
}
