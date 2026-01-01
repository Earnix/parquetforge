package com.earnix.parquet.columnar.s3.buffering;

import com.google.common.primitives.Longs;
import org.apache.commons.io.input.BoundedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Buffers part of a parquet file that can be uploaded to S3.
 */
public class S3FileUploadBuffer
{
	private static final Logger LOG = LoggerFactory.getLogger(S3FileUploadBuffer.class);
	private final int targetPartsPerRowGroup;
	private final Path tmpFile;
	private final FileChannel tmpFileChannel;

	public S3FileUploadBuffer(int targetPartsPerRowGroup, Path tmpFile) throws IOException
	{
		this.targetPartsPerRowGroup = targetPartsPerRowGroup;
		this.tmpFile = tmpFile;
		this.tmpFileChannel = FileChannel.open(tmpFile, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
	}

	public Path getFilePath()
	{
		return tmpFile;
	}

	/**
	 * Get the file channel to pass to the parquet data writer
	 *
	 * @return the file channel
	 */
	public FileChannel getTmpFileChannel()
	{
		return tmpFileChannel;
	}

	/**
	 * @param possibleUploadOffsets the offsets of this file where it is permissible to start a part. This avoids
	 *                              starting a new part in the middle of a data chunk.
	 * @return non-empty list of upload jobs, or empty optional if there is not enough data to upload
	 */
	public Optional<List<Runnable>> upload(long[] possibleUploadOffsets, S3KeyUploader uploader, boolean isLastPart)
	{
		if (possibleUploadOffsets == null || possibleUploadOffsets.length == 0)
			throw new IllegalArgumentException("cannot be empty");
		if (possibleUploadOffsets[0] == 0L)
			throw new IllegalArgumentException("Should provide end offsets ONLY");

		long[] possibleRanges = addZeroForStartRange(possibleUploadOffsets);
		long[] parts = UploadPartUtils.computePartDivisions(targetPartsPerRowGroup, possibleRanges);

		// in this case - we upload anyway because the last part has no minimum size.
		if (parts == null && isLastPart)
		{
			parts = new long[] { 0, lastElement(possibleUploadOffsets) };
		}

		if (parts == null)
			return Optional.empty();

		List<Runnable> uploadJobs = createRunnables(uploader, parts);
		return Optional.of(uploadJobs);
	}

	private static long[] addZeroForStartRange(long[] possibleUploadOffsets)
	{
		long[] possibleRanges = new long[possibleUploadOffsets.length + 1];
		possibleRanges[0] = 0L;
		System.arraycopy(possibleUploadOffsets, 0, possibleRanges, 1, possibleUploadOffsets.length);
		return possibleRanges;
	}

	private static long lastElement(long[] possibleUploadOffsets)
	{
		return possibleUploadOffsets[last(possibleUploadOffsets)];
	}

	private static int last(long[] possibleUploadOffsets)
	{
		return possibleUploadOffsets.length - 1;
	}

	private List<Runnable> createRunnables(S3KeyUploader uploader, long[] parts)
	{
		LOG.debug("Uploading {} job parts for {} with boundaries {}", parts.length - 1, uploader.getS3UploadUri(),
				Longs.asList(parts));

		List<Runnable> uploadJobs = new ArrayList<>(parts.length - 1);
		AtomicInteger countdown = new AtomicInteger(parts.length - 1);
		for (int uploadPart = 1; uploadPart < parts.length; uploadPart++)
		{
			int s3PartNum = uploader.getNextPartNum();
			long startOffset = parts[uploadPart - 1];
			long dataPagesLen = parts[uploadPart] - startOffset;
			Supplier<InputStream> inputStreamSupplier = () -> inputStreamInRange(this.tmpFile, startOffset,
					dataPagesLen);
			LOG.debug("Created upload job for {} partNum: {} startOffset: {} len: {}", uploader.getS3UploadUri(),
					s3PartNum, startOffset, dataPagesLen);

			Runnable uploadJob = () -> {
				try
				{
					uploader.uploadPart(s3PartNum, dataPagesLen, inputStreamSupplier);
					LOG.debug("Completed upload job for {} partNum: {} startOffset: {} len: {}", uploader.getS3UploadUri(),
							s3PartNum, startOffset, dataPagesLen);
				}
				finally
				{
					// the last thread to finish uploading should delete the tmp file.
					int remaining = countdown.decrementAndGet();
					if (remaining <= 0)
					{
						try
						{
							this.close();
							LOG.trace("Completed all upload jobs for {} in tmp file. Closing tmp file",
									uploader.getS3UploadUri());
						}
						catch (IOException ex)
						{
							// exception deleting tmp file - nothing we can do.
							LOG.warn("Error deleting temp file", ex);
						}
					}
				}
			};
			uploadJobs.add(uploadJob);
		}
		return uploadJobs;
	}

	private static InputStream inputStreamInRange(Path p, long startOffset, long len)
	{
		try
		{
			FileChannel fc = FileChannel.open(p);
			fc.position(startOffset);
			return BoundedInputStream.builder()//
					.setInputStream(Channels.newInputStream(fc))//
					.setMaxCount(len)//
					.setPropagateClose(true)//
					.get();
		}
		catch (IOException ex)
		{
			throw new UncheckedIOException(ex);
		}
	}

	public void close() throws IOException
	{
		try
		{
			closeChannel();
		}
		finally
		{
			try
			{
				Files.deleteIfExists(tmpFile);
			}
			catch (IOException exception)
			{
				LOG.warn("Error deleting tmpFile " + tmpFile, exception);
			}
		}
	}

	public void closeChannel() throws IOException
	{
		tmpFileChannel.close();
	}
}
