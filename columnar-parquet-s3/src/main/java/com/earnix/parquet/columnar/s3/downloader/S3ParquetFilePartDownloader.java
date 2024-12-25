package com.earnix.parquet.columnar.s3.downloader;

import com.earnix.parquet.columnar.utils.ParquetMagicUtils;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Util;
import software.amazon.awssdk.services.s3.model.ObjectPart;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;

/**
 * Download an s3 file into parts
 */
public class S3ParquetFilePartDownloader
{
	private static final AtomicLong threadPoolIDNumber = new AtomicLong();
	private static final int DEFAULT_DOWNLOAD_THREADS = 5;
	private final S3KeyDownloader s3KeyDownloader;
	private final int numDownloadThreads;
	private final RateLimiter rateLimiter;
	private volatile FileMetaData footerMetadata;

	public S3ParquetFilePartDownloader(S3KeyDownloader s3KeyDownloader)
	{
		this(s3KeyDownloader, DEFAULT_DOWNLOAD_THREADS, RateLimiter.create(5_000));
	}

	public S3ParquetFilePartDownloader(S3KeyDownloader s3KeyDownloader, int numDownloadThreads, RateLimiter rateLimiter)
	{
		this.s3KeyDownloader = s3KeyDownloader;
		this.numDownloadThreads = numDownloadThreads;
		this.rateLimiter = rateLimiter;
	}

	public FileMetaData getFileMetadata()
	{
		ensureFooterMetadataDownloaded();
		return footerMetadata.deepCopy();
	}

	private void ensureFooterMetadataDownloaded()
	{
		if (footerMetadata == null)
		{
			synchronized (this)
			{
				if (footerMetadata == null)
				{
					int lenAndMagicFooter = Integer.BYTES + ParquetMagicUtils.PARQUET_MAGIC.length();
					byte[] lastBytes = s3KeyDownloader.getLastBytes(lenAndMagicFooter);
					ByteBuffer buf = ByteBuffer.wrap(lastBytes).order(ByteOrder.LITTLE_ENDIAN);
					int footerMetadataLen = buf.getInt();
					// sanity check metadata that this indeed is a parquet file
					ParquetMagicUtils.expectMagic(buf);

					byte[] footerMetadataBytes = s3KeyDownloader.getLastBytes(footerMetadataLen + lenAndMagicFooter);
					try
					{
						footerMetadata = Util.readFileMetaData(new ByteArrayInputStream(footerMetadataBytes));
					}
					catch (IOException ex)
					{
						throw new UncheckedIOException(ex);
					}
				}
			}
		}
	}

	public void downloadToRowGroups(IntFunction<Path> rowGroupIntToPath) throws IOException
	{
		ensureFooterMetadataDownloaded();
		boolean success = false;
		Path[] rowGroupPath = null;
		try
		{
			rowGroupPath = createFiles(rowGroupIntToPath);
			long[] rowGrpStartToFileOffset = computeStartingOffsetsForRowGroups();

			ExecutorService service = new ThreadPoolExecutor(numDownloadThreads, numDownloadThreads, 0L,
					TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), new ThreadFactoryBuilder().setNameFormat(
					"parquet-s3-downloader-" + threadPoolIDNumber.incrementAndGet() + "-%d").build());
			try
			{
				long[] partsListAbsoluteOffsets = computePartOffsets();
				sanityCheck(partsListAbsoluteOffsets[0] == 0, "first part must be at the beginning of the object");
				sanityCheck(partsListAbsoluteOffsets[partsListAbsoluteOffsets.length - 1]
						== s3KeyDownloader.getObjectSize(), "Last part MUST end a the length of the s3 object");

				parallelDownload(service, partsListAbsoluteOffsets, rowGroupPath, rowGrpStartToFileOffset);
			}
			finally
			{
				try
				{
					service.shutdown();
					service.awaitTermination(365, TimeUnit.DAYS);
				}
				catch (InterruptedException ex)
				{
					// interrupted in finally. interrupt the thread, and don't throw.
					service.shutdownNow();
					Thread.currentThread().interrupt();
				}
			}
			success = true;
		}
		finally
		{
			if (!success)
			{
				deleteAll(rowGroupPath);
			}
		}
	}

	private void parallelDownload(ExecutorService service, long[] partsListAbsoluteOffsets, Path[] rowGroupPath,
			long[] rowGrpStartToFileOffset) throws IOException
	{
		for (int i = 0; i < partsListAbsoluteOffsets.length - 1; i++)
		{
			final long[] byteRange = { partsListAbsoluteOffsets[i], partsListAbsoluteOffsets[i + 1] };

			// find first path containing these bytes
			int pos = Arrays.binarySearch(partsListAbsoluteOffsets, byteRange[0]);

			// this can happen with the first part, because we discard the magic
			if (pos == -1)
			{
				byteRange[0] = partsListAbsoluteOffsets[0];
			}
			if (pos < 0)
			{
				pos = -pos - 1;
			}

			sanityCheck(partsListAbsoluteOffsets[pos] <= byteRange[0], "Wrong part found");

			int startRowgroupOffset = pos;
			s3KeyDownloader.downloadRange(byteRange[0], byteRange[1], is -> {
				long currentBytePos = byteRange[0];
				for (int currFile = startRowgroupOffset; currentBytePos < byteRange[1]; currFile++)
				{
					try (FileChannel fc = FileChannel.open(rowGroupPath[currFile], StandardOpenOption.WRITE))
					{
						long offsetInRowGroup = currentBytePos - rowGrpStartToFileOffset[currFile];
						fc.position(ParquetMagicUtils.PARQUET_MAGIC.length() + offsetInRowGroup);
						long len = Math.min(byteRange[1], rowGrpStartToFileOffset[currFile + 1] - currentBytePos);
						long bytesCopied = IOUtils.copyLarge(is, Channels.newOutputStream(fc), 0, len);
						sanityCheck(bytesCopied == len,
								"Bytes copied does not match len: " + len + " " + "bytesCopied: " + bytesCopied);
						currentBytePos += len;
					}
				}
			});
		}

		//TODO: write footer metadata in each of the created files
	}

	private long[] computePartOffsets()
	{
		long[] partsListAbsoluteOffsets = new long[s3KeyDownloader.getPartsList().size() + 1];
		int offsetIdx = 1;
		for (ObjectPart objectPart : s3KeyDownloader.getPartsList())
		{
			partsListAbsoluteOffsets[offsetIdx] = partsListAbsoluteOffsets[offsetIdx - 1] + objectPart.size();
			offsetIdx++;
		}
		return partsListAbsoluteOffsets;
	}

	private static void deleteAll(Path[] rowGroupPath)
	{
		if (rowGroupPath != null)
		{
			for (Path p : rowGroupPath)
			{
				try
				{
					if (p != null)
					{
						Files.deleteIfExists(p);
					}
				}
				catch (IOException ex)
				{
					// eat it - we're in a finally block.
				}
			}
		}
	}

	private long[] computeStartingOffsetsForRowGroups()
	{
		long[] rowGrpStartToFileOffset = new long[footerMetadata.getRow_groupsSize()];
		Iterator<RowGroup> it2 = footerMetadata.getRow_groupsIterator();
		for (int rowGrpNum = 0; it2.hasNext(); rowGrpNum++)
		{
			RowGroup rowGroup = it2.next();
			rowGrpStartToFileOffset[rowGrpNum] = rowGroup.getFile_offset();
		}
		return rowGrpStartToFileOffset;
	}

	private Path[] createFiles(IntFunction<Path> rowGroupIntToPath) throws IOException
	{
		Path[] rowGroupPath = new Path[footerMetadata.getRow_groupsSize()];
		boolean success = false;
		try
		{
			Iterator<RowGroup> it = footerMetadata.getRow_groupsIterator();
			for (int rowGrpNum = 0; it.hasNext(); rowGrpNum++)
			{
				// create and write magic
				rowGroupPath[rowGrpNum] = Files.write(rowGroupIntToPath.apply(rowGrpNum),
						ParquetMagicUtils.PARQUET_MAGIC.getBytes(StandardCharsets.US_ASCII),
						StandardOpenOption.CREATE_NEW);
			}
			success = true;
			return rowGroupPath;
		}
		finally
		{
			if (!success)
			{
				deleteAll(rowGroupPath);
			}
		}
	}

	private void sanityCheck(boolean assertion, String message)
	{
		if (!assertion)
			throw new IllegalStateException(message);
	}
}
