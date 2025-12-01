package com.earnix.parquet.columnar.file.reader;

import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.io.input.BoundedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * A class that supples an input stream for a limited byte range within a file
 */
public class FileRangeInputStreamSupplier implements IOSupplier<InputStream>
{
	private final Path p;
	private final long startOffset;
	private final long numBytesToRead;

	/**
	 * @param p           the path to construct the input streams supplier for
	 * @param startOffset the start offset in the file
	 * @param numBytesToRead         the length of the input stream
	 */
	public FileRangeInputStreamSupplier(Path p, long startOffset, long numBytesToRead)
	{
		this.p = p;
		this.startOffset = startOffset;
		this.numBytesToRead = numBytesToRead;
	}

	/**
	 * Return a newly opened input stream for this specific range in the file. Note: you MUST close the input stream!
	 *
	 * @return the input stream.
	 * @throws IOException on failure to open the file
	 */
	public InputStream get() throws IOException
	{
		boolean success = false;
		FileChannel fc = FileChannel.open(p);
		try
		{
			sanityCheckFileLen(fc);

			// seek the channel to the start offset
			fc.position(startOffset);

			// limit to len bytes
			InputStream ret = BoundedInputStream.builder().setInputStream(Channels.newInputStream(fc)).setMaxCount(
							numBytesToRead)
					.get();
			success = true;
			return ret;
		}
		finally
		{
			// in case an exception is thrown during the seek, close the channel. If not, the caller should close
			// the input stream which will in turn close the backing file channel.
			if (!success)
				fc.close();
		}
	}

	private void sanityCheckFileLen(FileChannel fc) throws IOException
	{
		long fileLen = fc.size();
		if (fileLen < startOffset)
		{
			throw new IOException("File " + p + " ends before startOffset: " + startOffset + " size: " + fileLen);
		}
		if (fileLen < startOffset + numBytesToRead)
		{
			throw new IOException(
					"File " + p + " ends before end offset: " + startOffset + " len: " + numBytesToRead + " " + "size: "
							+ fileLen);
		}
	}

	/**
	 * Get the path of the supplied file
	 *
	 * @return the path of the file
	 */
	public Path getPath()
	{
		return p;
	}

	/**
	 * Get the start offset in the file
	 *
	 * @return the start offset in the file
	 */
	public long getStartOffset()
	{
		return startOffset;
	}

	/**
	 * Get the number of bytes to read past the starting offset
	 *
	 * @return the number of bytes
	 */
	public long getNumBytesToRead()
	{
		return numBytesToRead;
	}
}
