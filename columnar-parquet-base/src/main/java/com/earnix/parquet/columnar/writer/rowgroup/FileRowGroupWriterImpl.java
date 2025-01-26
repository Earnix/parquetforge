package com.earnix.parquet.columnar.writer.rowgroup;

import com.earnix.parquet.columnar.writer.columnchunk.ChunkWritingUtils;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkPages;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriter;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriterImpl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class FileRowGroupWriterImpl implements RowGroupWriter
{
	private final MessageType messageType;

	private final FileChannel output;
	private final Path outputFile;

	private final CompressionCodec compressionCodec;
	private final ColumnChunkWriter columnChunkWriter;
	private final long numRows;
	private final long startingOffset;
	private final AtomicLong currOffset;
	private final List<ColumnChunkInfo> chunkInfoList;
	private volatile boolean closed = false;

	/**
	 * @param messageType       the message type used to validate all columns in this row group are written. If null,
	 *                          validation is skipped
	 * @param compressionCodec  the compression codec to use
	 * @param parquetProperties tuning parameters for column writing
	 * @param numRows           the number of rows in this row group
	 * @param outputFile        The file to write the data to. Each column chunk open and closes the file
	 * @param output            the file channel to output the data to. If null, the file will be open and closed every
	 *                          time it needs to be written to. It may carry some addtional filesystem overhead
	 * @param startingOffset    the starting offset in the output file
	 */
	public FileRowGroupWriterImpl(MessageType messageType, CompressionCodec compressionCodec,
			ParquetProperties parquetProperties, long numRows, Path outputFile, FileChannel output, long startingOffset)
	{
		this.messageType = messageType;

		this.output = output;
		this.outputFile = outputFile;

		this.compressionCodec = compressionCodec;
		this.columnChunkWriter = new ColumnChunkWriterImpl(compressionCodec, parquetProperties);
		this.numRows = numRows;
		this.startingOffset = startingOffset;
		this.currOffset = new AtomicLong(startingOffset);
		this.chunkInfoList = Collections.synchronizedList(new ArrayList<>());
	}

	@Override
	public void writeValues(ChunkValuesWritingFunction writer) throws IOException
	{
		ColumnChunkPages pages = writer.apply(columnChunkWriter);
		long totalBytes = pages.totalBytesForStorage();
		long writeOffset = this.currOffset.getAndAdd(totalBytes);
		if (this.output != null)
		{
			pages.writeToFile(output, writeOffset);
		}
		else
		{
			try (FileChannel fc = FileChannel.open(outputFile, StandardOpenOption.WRITE))
			{
				pages.writeToFile(fc, writeOffset);
			}
		}
		validateNumRows(pages);
		chunkInfoList.add(new PartialColumnChunkInfo(pages, computeStartingOffset(writeOffset)));
		assertNotClosed();
	}

	protected long computeStartingOffset(long startingOffset)
	{
		return startingOffset;
	}

	@Override
	public void writeCopyOfChunk(ColumnDescriptor columnDescriptor, ColumnChunk columnChunk,
			InputStream chunkInputStream)
	{
		try
		{
			long chunkTotalBytes = columnChunk.getMeta_data().getTotal_compressed_size();
			// For thread safety, moving the position for the next thread to the end of
			// this chunk and then writing the chunk itself
			long startingOffset = this.currOffset.getAndAdd(chunkTotalBytes);

			writeToChannel(chunkInputStream, startingOffset, chunkTotalBytes);
			chunkInfoList.add(new FullColumnChunkInfo(columnDescriptor, columnChunk, startingOffset));
		}
		catch (IOException ex)
		{
			throw new UncheckedIOException(ex);
		}
	}

	private void writeToChannel(InputStream chunkInputStream, long startingOffset, long chunkTotalBytes)
			throws IOException
	{
		if (this.output == null)
		{
			BoundedInputStream boundedInputStream = BoundedInputStream.builder().setInputStream(chunkInputStream)
					.setMaxCount(chunkTotalBytes).get();
			try (FileChannel fc = FileChannel.open(outputFile, StandardOpenOption.WRITE))
			{
				fc.position(startingOffset);
				OutputStream os = Channels.newOutputStream(fc);
				long bytesWritten = IOUtils.copyLarge(boundedInputStream, os);
				if (bytesWritten != chunkTotalBytes)
				{
					throw new IllegalStateException(
							"Unexpected number of bytes written expected: " + chunkTotalBytes + " actual: "
									+ bytesWritten);
				}
			}
		}
		else
		{
			// thread safe code to reuse the file channel without touching its position.
			byte[] buf = new byte[8192];
			ByteBuffer byteBuffer = ByteBuffer.wrap(buf);

			for (long idx = 0; idx < chunkTotalBytes; idx += buf.length)
			{
				byteBuffer.clear();
				int len = Math.toIntExact(Math.min(buf.length, chunkTotalBytes - idx));
				IOUtils.readFully(chunkInputStream, buf, 0, len);
				byteBuffer.limit(len);

				ChunkWritingUtils.writeByteBufferToChannelFully(this.output, byteBuffer, startingOffset + idx);
			}
		}
	}

	private void validateNumRows(ColumnChunkPages pages)
	{
		if (pages.getNumValues() != numRows)
		{
			throw new IllegalStateException(
					"The number of rows in the chunk is not the one that was declared for the row group"
							+ pages.getColumnDescriptor().getPrimitiveType().getName());
		}
	}

	public RowGroupInfo closeAndValidateAllColumnsWritten() throws IOException
	{
		assertNotClosed();
		this.closed = true;
		Set<ColumnDescriptor> descriptors = chunkInfoList.stream().map(ColumnChunkInfo::getDescriptor)
				.collect(Collectors.toSet());
		if (messageType != null && !new HashSet<>(messageType.getColumns()).equals(descriptors))
		{
			throw new IllegalStateException(
					"Not all columns in this row group were written. required: " + messageType.getColumns() + " "
							+ "actual: " + descriptors);
		}

		if (this.output != null)
			this.output.position(this.currOffset.get());

		long len = this.currOffset.get() - this.startingOffset;
		return new RowGroupInfo(computeStartingOffset(startingOffset), len, numRows, chunkInfoList);
	}

	private void assertNotClosed()
	{
		if (closed)
			throw new IllegalStateException("Closed");
	}
}
