package com.earnix.parquet.columnar.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
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

	public static InputStream newMagicBytesInputStream()
	{
		return new ByteArrayInputStream(PARQUET_MAGIC_BYTES);
	}

	public static void writeMagicBytes(WritableByteChannel fileChannel) throws IOException
	{
		ByteBuffer bb = ByteBuffer.wrap(PARQUET_MAGIC_BYTES);
		do
		{
			int bytesWritten = fileChannel.write(bb);
			if (bytesWritten <= 0)
				throw new IOException("Could not write magic bytes");
		}
		while (bb.hasRemaining());
	}

	public static byte[] createFooterAndMagic(int metadataSize)
	{
		byte[] footerLenAndMagic = new byte[Integer.BYTES + PARQUET_MAGIC.length()];
		ByteBuffer footerLenAndMagicByteBuf = ByteBuffer.wrap(footerLenAndMagic);
		footerLenAndMagicByteBuf.order(ByteOrder.LITTLE_ENDIAN);// little endian according to spec.
		footerLenAndMagicByteBuf.putInt(metadataSize);
		footerLenAndMagicByteBuf.put(PARQUET_MAGIC_BYTES);
		if (footerLenAndMagicByteBuf.hasRemaining())
		{
			throw new IllegalStateException();
		}
		return footerLenAndMagic;
	}
}
