package com.earnix.parquet.columnar.writer.compressors;

import java.io.IOException;

import org.xerial.snappy.Snappy;

public class CompressorSnappyImpl implements Compressor
{
	@Override
	public int maxCompressedLength(int numBytes)
	{
		return Snappy.maxCompressedLength(numBytes);
	}

	@Override
	public int compress(byte[] input, byte[] output)
	{
		try
		{
			return Snappy.compress(input, 0, input.length, output, 0);
		}
		catch (IOException ex)
		{
			throw new RuntimeException(ex);
		}
	}
}
