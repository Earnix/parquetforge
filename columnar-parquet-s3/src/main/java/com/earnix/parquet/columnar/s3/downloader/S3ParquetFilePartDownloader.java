package com.earnix.parquet.columnar.s3.downloader;

import com.earnix.parquet.columnar.utils.ParquetMagicUtils;
import com.earnix.parquet.columnar.writer.ParquetWriterUtils;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.parquet.format.ColumnChunk;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

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

	public void downloadToRowGroups(BiFunction<Integer, RowGroup, Path> rowGroupIntToPath) throws IOException
	{
		ensureFooterMetadataDownloaded();
		sanityCheckRowGroupsAreContiguous();

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
			catch (InterruptedException ex)
			{
				Thread.currentThread().interrupt();
				throw new IllegalStateException("Interrupted");
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
			long[] rowGrpStartToFileOffset) throws IOException, InterruptedException
	{
		Future<?>[] futs = new Future[partsListAbsoluteOffsets.length - 1];
		RowGroup lastRG = footerMetadata.getRow_groups().get(footerMetadata.getRow_groupsSize() - 1);
		long startOffsetOfFooterMetadata = lastRG.getFile_offset() + lastRG.getTotal_compressed_size();
		for (int i = 0; i < partsListAbsoluteOffsets.length - 1; i++)
		{
			final long[] byteRange = { partsListAbsoluteOffsets[i], partsListAbsoluteOffsets[i + 1] };
			sanityCheck(byteRange[0] < byteRange[1], "Byte range must be have positive length");

			// if this part starts at or after the footer metadata, we have no need to download it. We're done!
			if (byteRange[0] >= startOffsetOfFooterMetadata)
			{
				// fill the futures array with completed futures to prevent NPE.
				futs[i] = CompletableFuture.completedFuture(null);
				continue;
			}

			// find first path containing these bytes
			int pos = Arrays.binarySearch(rowGrpStartToFileOffset, byteRange[0]);

			// this can happen with the first part, because we discard the magic
			if (pos == -1)
			{
				byteRange[0] = rowGrpStartToFileOffset[0];
				pos = 0;
			}
			else if (pos < 0)
			{
				pos = -pos - 2;
			}

			sanityCheck(rowGrpStartToFileOffset[pos] <= byteRange[0], "Wrong row group found");
			sanityCheck(pos == rowGrpStartToFileOffset.length - 1 || rowGrpStartToFileOffset[pos + 1] > byteRange[0],
					"Wrong row group found");

			int startRowgroupOffset = pos;
			rateLimiter.acquire();
			futs[i] = service.submit(() -> {
				downloadRange(rowGroupPath, rowGrpStartToFileOffset, byteRange, startRowgroupOffset);
				return null;
			});
		}

		waitForFutures(futs);

		//TODO: write footer metadata in each of the created files
		for (int rowGrpNum = 0; rowGrpNum < rowGroupPath.length; rowGrpNum++)
		{
			FileMetaData newFooterMetadata = footerMetadata.deepCopy();
			RowGroup rg = newFooterMetadata.getRow_groups().get(rowGrpNum);
			long delta = ParquetMagicUtils.PARQUET_MAGIC.length() - rg.getFile_offset();
			rg.setFile_offset(ParquetMagicUtils.PARQUET_MAGIC.length());
			Iterator<ColumnChunk> it = rg.getColumnsIterator();
			while (it.hasNext())
			{
				ColumnChunk columnChunk = it.next();
				columnChunk.getMeta_data()
						.setData_page_offset(columnChunk.getMeta_data().getData_page_offset() + delta);
			}

			// this is the only row group
			newFooterMetadata.setRow_groups(Collections.singletonList(rg));
			try (FileChannel fc = FileChannel.open(rowGroupPath[rowGrpNum], StandardOpenOption.WRITE))
			{
				fc.position(fc.size());
				ParquetWriterUtils.writeFooterMetadataAndMagic(fc, newFooterMetadata);
			}
		}
	}

	private static void waitForFutures(Future<?>[] futs) throws InterruptedException, IOException
	{
		for (Future<?> fut : futs)
		{
			try
			{
				fut.get();
			}
			catch (ExecutionException ex)
			{
				if (ex.getCause() instanceof RuntimeException)
					throw (RuntimeException) ex.getCause();
				if (ex.getCause() instanceof IOException)
					throw (IOException) ex.getCause();

				// shouldn't happen..
				throw new IllegalStateException(ex);
			}
		}
	}

	private void downloadRange(Path[] rowGroupPath, long[] rowGrpStartToFileOffset, long[] byteRange,
			int startRowgroupOffset) throws IOException
	{
		s3KeyDownloader.downloadRange(byteRange[0], byteRange[1], is -> {
			long currentBytePos = byteRange[0];
			for (int rowGroupNum = startRowgroupOffset;
				 currentBytePos < byteRange[1] && rowGroupNum < rowGrpStartToFileOffset.length; rowGroupNum++)
			{
				try (FileChannel fc = FileChannel.open(rowGroupPath[rowGroupNum], StandardOpenOption.WRITE))
				{
					long offsetInRowGroup = currentBytePos - rowGrpStartToFileOffset[rowGroupNum];

					// note this will break if the parquet file has some buffer space between row groups..
					sanityCheck(offsetInRowGroup >= 0, "Offset in row group must be greater than zero");
					long rowGroupLength = footerMetadata.getRow_groups().get(rowGroupNum).getTotal_compressed_size();
					sanityCheck(offsetInRowGroup < rowGroupLength,
							"Offset in row group must be less than total compressed size");

					fc.position(ParquetMagicUtils.PARQUET_MAGIC.length() + offsetInRowGroup);

					long len = Math.min(byteRange[1] - currentBytePos, rowGroupLength - offsetInRowGroup);
					sanityCheck(len > 0, "len must be greater than zero");
					long bytesCopied = IOUtils.copyLarge(is, Channels.newOutputStream(fc), 0, len);
					sanityCheck(bytesCopied == len,
							"Bytes copied does not match len: " + len + " " + "bytesCopied: " + bytesCopied);
					currentBytePos += len;
				}
			}
		});
	}

	private void sanityCheckRowGroupsAreContiguous()
	{
		ensureFooterMetadataDownloaded();
		Iterator<RowGroup> it = footerMetadata.getRow_groupsIterator();

		long expectedOffset = ParquetMagicUtils.PARQUET_MAGIC.length();
		while (it.hasNext())
		{
			RowGroup rg = it.next();
			sanityCheck(rg.getFile_offset() == expectedOffset, "Row group start at unexpected place");
			sanityCheck(rg.getTotal_compressed_size() > 0, "Row group compressed size invalid");
			expectedOffset += rg.getTotal_compressed_size();

			Iterator<ColumnChunk> columnChunkIterator = rg.getColumnsIterator();
			while (columnChunkIterator.hasNext())
			{
				ColumnChunk columnChunk = columnChunkIterator.next();
				sanityCheck(columnChunk.getMeta_data().getData_page_offset() >= rg.getFile_offset(),
						"chunk cannot start before row group offset");
				sanityCheck(columnChunk.getMeta_data().getTotal_compressed_size() > 0,
						"chunk must have more than zero data");
				long chunkEnd = columnChunk.getMeta_data().getData_page_offset() + columnChunk.getMeta_data()
						.getTotal_compressed_size();
				sanityCheck(chunkEnd <= expectedOffset, "chunk cannot end after row group offset");
			}
		}
	}

	/**
	 * Compute the starting part offsets to fetch from S3 when downloading. The default implementation uses the parts
	 * from list parts for optimal performance. This is protected in order to easily test randomized part breakdowns to
	 * find corner cases.
	 * <br>
	 * In the event that parts are not found, it will return a single offset with the whole file. We should consult with
	 * the S3 engineers to understand whether parallelizing a download makes sense in this case.
	 *
	 * @return the starting and ending part offsets
	 */
	protected long[] computePartOffsets()
	{
		List<ObjectPart> partsList = s3KeyDownloader.getPartsList();

		// if the object was not uploaded in parts, use only one part to download. Need to ask AWS if this is good..
		if (partsList.isEmpty())
			return new long[] { 0, s3KeyDownloader.getObjectSize() };

		long[] partsListAbsoluteOffsets = new long[partsList.size() + 1];
		int offsetIdx = 1;
		for (ObjectPart objectPart : partsList)
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

	private Path[] createFiles(BiFunction<Integer, RowGroup, Path> rowGroupIntToPath) throws IOException
	{
		Path[] rowGroupPath = new Path[footerMetadata.getRow_groupsSize()];
		boolean success = false;
		try
		{
			Iterator<RowGroup> it = footerMetadata.getRow_groupsIterator();
			for (int rowGrpNum = 0; it.hasNext(); rowGrpNum++)
			{
				// create and write magic
				rowGroupPath[rowGrpNum] = Files.write(rowGroupIntToPath.apply(rowGrpNum, it.next()),
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
