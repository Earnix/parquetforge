package com.earnix.parquet.columnar.s3;

import com.earnix.parquet.columnar.s3.buffering.S3FileUploadBuffer;
import com.earnix.parquet.columnar.s3.buffering.S3KeyUploader;
import com.earnix.parquet.columnar.utils.ParquetMagicUtils;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import com.earnix.parquet.columnar.writer.ParquetFileInfo;
import com.earnix.parquet.columnar.writer.ParquetWriterUtils;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupInfo;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupWriter;
import org.apache.commons.io.file.DeletingPathVisitor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This is a class for writing a parquet file directly to S3, buffering RowGroups on disk before uploading. Large files
 * are not completely buffered on disk - once the parts of the file are written to S3, they are removed from local
 * disk.
 * <br>
 * The parquet file is initially buffered onto disk before being uploaded to S3. Once a RowGroup is finished, there is
 * an algorithm that determines at what offsets to upload the RowGroup to S3, such that the parts on S3 start and end at
 * a boundary between two column's data pages. But not necessarily between two RowGroups, as this isn't possible if a
 * RowGroup is less than {@link S3Constants#MIN_S3_PART_SIZE}
 */
public class ParquetS3ObjectWriterImpl implements ParquetColumnarWriter
{
	private static final int DAYS_IN_A_YEAR = 365;

	// These should be configurable.
	private static final int NUM_UPLOAD_THREADS = 3;

	private final ExecutorService executor;
	private final S3KeyUploader s3KeyUploader;
	private final int targetS3PartsPerRowGroup;

	// TODO: this needs to be a base class or wrapped elsewhere - dupe code in file writer
	private final MessageType messageType;
	private final ParquetProperties parquetProperties;
	private final CompressionCodec compressionCodec;

	private final boolean deleteTmpFolder;
	private final Path tmpFolder;

	// offset from all previous buffers that have been uploaded.
	private long offsetOfCurrentFile = 0L;
	private final List<RowGroupInfo> rowGroupInfos = new ArrayList<>();
	private S3RowGroupWriterImpl lastWriter = null;

	private S3FileUploadBuffer buffer;

	private static Path createTmpFolder() throws IOException
	{
		return Files.createTempDirectory("parquet_s3_upload");
	}

	public ParquetS3ObjectWriterImpl(MessageType messageType, CompressionCodec compressionCodec,
			ParquetProperties parquetProperties, S3KeyUploader s3KeyUploader, int targetS3PartsPerRowGroup)
			throws IOException
	{
		this(s3KeyUploader, targetS3PartsPerRowGroup, messageType, parquetProperties, compressionCodec, true,
				createTmpFolder());
	}

	/**
	 * Construct a parquet s3 object writer
	 *
	 * @param tmpFolder              a temp folder to put the temp files for buffering uploads
	 * @param compressionCodec       the compression codec to use.
	 * @param parquetProperties      the parquet properties when creating the parquet writer class
	 * @param messageType            the schema of the file
	 * @param s3KeyUploader          the s3 key uploader - where the parquet file is written
	 * @param targetPartsPerRowGroup the target number of parts to attempt to upload per row group. This parameter MUST
	 *                               be set in accordance to the number of row groups that will be uploaded. If it is
	 *                               set too low, there will be very few parts to download which may negatively impact
	 *                               download performance. If set too high, the S3 upload will fail if more than 10000
	 *                               parts are needed.
	 */
	public ParquetS3ObjectWriterImpl(Path tmpFolder, CompressionCodec compressionCodec,
			ParquetProperties parquetProperties, MessageType messageType, S3KeyUploader s3KeyUploader,
			int targetPartsPerRowGroup)
	{
		this(s3KeyUploader, targetPartsPerRowGroup, messageType, parquetProperties, compressionCodec, false, tmpFolder);
	}

	private ParquetS3ObjectWriterImpl(S3KeyUploader s3KeyUploader, int targetPartsPerRowGroup, MessageType messageType,
			ParquetProperties parquetProperties, CompressionCodec compressionCodec, boolean deleteTmpFolder,
			Path tmpFolder)
	{
		this.s3KeyUploader = s3KeyUploader;
		this.targetS3PartsPerRowGroup = targetPartsPerRowGroup;
		this.messageType = messageType;
		this.parquetProperties = parquetProperties;
		this.compressionCodec = compressionCodec;
		this.deleteTmpFolder = deleteTmpFolder;
		this.tmpFolder = tmpFolder;

		this.executor = new ThreadPoolExecutor(1, NUM_UPLOAD_THREADS, 0L, TimeUnit.MILLISECONDS,
				new SynchronousQueue<>(), new ThreadPoolExecutor.CallerRunsPolicy());
	}

	public S3RowGroupWriterImpl startNewRowGroup(long numRows) throws IOException
	{
		if (lastWriter != null)
		{
			throw new IllegalStateException("Last writer was not closed");
		}

		if (buffer == null)
		{
			makeNewBuffer();
			if (rowGroupInfos.isEmpty())
			{
				// this is the first chunk - need to add the magic bytes
				ParquetMagicUtils.writeMagicBytes(buffer.getTmpFileChannel());
			}
		}

		lastWriter = new S3RowGroupWriterImpl(messageType, compressionCodec, parquetProperties, numRows,
				buffer.getFilePath(), buffer.getTmpFileChannel(), buffer.getTmpFileChannel().position(),
				offsetOfCurrentFile);
		return lastWriter;
	}

	private void makeNewBuffer() throws IOException
	{
		Path tmpFile = Files.createTempFile(tmpFolder, "parquet_s3_part", ".bin");
		buffer = new S3FileUploadBuffer(targetS3PartsPerRowGroup, tmpFile);
	}

	@Override
	public void finishRowGroup() throws IOException
	{
		RowGroupInfo rowGroupInfo = lastWriter.closeAndValidateAllColumnsWritten();
		this.rowGroupInfos.add(rowGroupInfo);

		// the possible split points array contains the ending points of each column within a row group.
		long[] possibleSplitPoints = rowGroupInfo.getCols().stream()//
				.mapToLong(col -> col.getStartingOffset() + col.getCompressedSize() - this.offsetOfCurrentFile)
				.toArray();

		// We sort it as there's no guarantee of what order the RowGroup parts were in.
		Arrays.sort(possibleSplitPoints);

		uploadBufferedData(possibleSplitPoints, false);
		lastWriter = null;
	}

	@Override
	public RowGroupWriter getCurrentRowGroupWriter()
	{
		return lastWriter;
	}

	private void uploadBufferedData(long[] possibleSplitPoints, boolean isLastPart) throws IOException
	{
		Optional<List<Runnable>> uploadJobs = buffer.upload(possibleSplitPoints, s3KeyUploader, isLastPart);

		if (uploadJobs.isPresent())
		{
			buffer.closeChannel();
			buffer = null;

			// TODO: we need to add error handling - if an upload job has an exception, it doesn't bubble back here.
			for (Runnable uploadJob : uploadJobs.get())
			{
				executor.submit(uploadJob);
			}

			offsetOfCurrentFile += possibleSplitPoints[possibleSplitPoints.length - 1];
		}
	}

	@Override
	public ParquetFileInfo finishAndWriteFooterMetadata() throws IOException
	{
		if (buffer == null)
			makeNewBuffer();

		FileMetaData fileMetaData = ParquetWriterUtils.getFileMetaData(messageType, rowGroupInfos);
		int numFooterBytes = ParquetWriterUtils.writeFooterMetadataAndMagic(buffer.getTmpFileChannel(), fileMetaData);
		uploadBufferedData(new long[] { buffer.getTmpFileChannel().position() }, true);

		waitForAsyncUploadJobs();
		s3KeyUploader.finish();
		return new ParquetFileInfo(offsetOfCurrentFile - numFooterBytes, offsetOfCurrentFile, messageType,
				fileMetaData);
	}

	private void waitForAsyncUploadJobs() throws IOException
	{
		// wait for upload
		executor.shutdown();
		try
		{
			// TODO: shouldn't happen.. perhaps this should be configurable??
			if (!executor.awaitTermination(DAYS_IN_A_YEAR, TimeUnit.DAYS))
			{
				throw new IOException("upload timed out");
			}
		}
		catch (InterruptedException ex)
		{
			Thread.currentThread().interrupt();
			throw new IOException("upload was interrupted");
		}
	}

	@Override
	public synchronized void close() throws IOException
	{
		if (buffer != null)
		{
			try
			{
				buffer.close();
			}
			catch (Exception ex)
			{
				// ignore
			}
		}
		if (deleteTmpFolder && Files.exists(tmpFolder))
		{
			// delete tmp folder and any contained files.
			Files.walkFileTree(tmpFolder, DeletingPathVisitor.withLongCounters());
		}
	}
}
