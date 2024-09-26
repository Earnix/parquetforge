package com.earnix.parquet.columnar.utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A class to store the parquet magic bytes
 */
public class ParquetMagicUtils
{
	/**
	 * The parquet magic string
	 */
	public static final String PARQUET_MAGIC = "PAR1";
	private static final byte[] PARQUET_MAGIC_BYTES = PARQUET_MAGIC.getBytes(StandardCharsets.US_ASCII);

	public static void writeMagicToBuf(ByteBuffer b)
	{

	}

	/**
	 * Returns whether magic was contained in the byte buffer
	 * 
	 * @param buf the byte buffer to check
	 * @return whether the magic was contained
	 */
	public static boolean expectMagic(ByteBuffer buf)
	{
		if (buf.remaining() < PARQUET_MAGIC_BYTES.length)
			return false;
		for (byte magicByte : PARQUET_MAGIC_BYTES)
		{
			if (buf.get() != magicByte)
				return false;
		}
		return true;
	}
}
