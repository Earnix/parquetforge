package com.earnix.parquet.columnar.reader;

import org.apache.commons.io.input.BoundedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class FileRangeInputStreamSupplier
{
	private final Path p;
	private final long startOffset;
	private final long len;

	public FileRangeInputStreamSupplier(Path p, long startOffset, long len)
	{
		this.p = p;
		this.startOffset = startOffset;
		this.len = len;
	}

	public InputStream get() throws IOException
	{
		boolean success = false;
		FileChannel fc = FileChannel.open(p);
		try
		{
			fc.position(startOffset);
			InputStream ret = BoundedInputStream.builder() // limit to len bytes
					.setInputStream(Channels.newInputStream(fc)).setMaxCount(len).get();
			success = true;
			return ret;
		}
		finally
		{
			// in case an exception is thrown during the seek, close the channel. If not, the caller should close
			// the input stream.
			if (!success)
				fc.close();
		}
	}

	public Path getPath()
	{
		return p;
	}

	public long getStartOffset()
	{
		return startOffset;
	}

	public long getLen()
	{
		return len;
	}
}
