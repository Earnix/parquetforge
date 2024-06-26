package com.earnix.parquet.columnar.compressors;

public interface Compressor
{
	/**
	 * Compress data with this compressor
	 * 
	 * @param input data to compress
	 * @param output output buffer for compressed data. This buffer must be at least maxCompressedLength(input.length)
	 * @return number of bytes used in the output array
	 */
	int compress(byte[] input, byte[] output);

	/**
	 * The max number of bytes that an arbitrary byte array of this size will take once compressed
	 * 
	 * @param numBytes number of bytes in the array to compress
	 * @return number of bytes that the compression will use worst case scenario
	 */
	int maxCompressedLength(int numBytes);
}
