package com.earnix.parquet.columnar.writer.columnchunk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ChunkWritingUtils
{
	/**
	 * Write the byte buffer to the file channel fully, throwing an exception if this is not possible
	 * @param fc the file channel to write to
	 * @param bb the byte buffer to read from
	 * @param offset the offset in the file to write to
	 * @return the number of bytes written
	 * @throws IOException
	 */
	public static int writeByteBufferToChannelFully(FileChannel fc, ByteBuffer bb, long offset) throws IOException
	{
		int totalBytesWritten = 0;
		while (bb.hasRemaining())
		{
			int written = fc.write(bb, offset);
			if (0 == written)
			{
				throw new IOException("Nothing got written .. should not occur");
			}
			totalBytesWritten += written;
		}
		return totalBytesWritten;
	}
}
