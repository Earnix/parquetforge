package com.earnix.parquet.columnar.writer.rowgroup;

import com.earnix.parquet.columnar.writer.columnchunk.ChunkWritingUtils;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkPages;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriter;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriterImpl;
import org.apache.commons.io.IOUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
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
	private final CompressionCodec compressionCodec;
	private final ColumnChunkWriter columnChunkWriter;
	private final long numRows;
	private final long startingOffset;
	private final AtomicLong currOffset;
	private final List<ColumnChunkInfo> chunkInfoList;
	private volatile boolean closed = false;

	public FileRowGroupWriterImpl(MessageType messageType, CompressionCodec compressionCodec,
			ParquetProperties parquetProperties, long numRows, FileChannel output)
	{
		this.messageType = messageType;
		this.output = output;
		this.compressionCodec = compressionCodec;
		this.columnChunkWriter = new ColumnChunkWriterImpl(messageType, compressionCodec, parquetProperties, numRows);
		this.numRows = numRows;
		try
		{
			this.startingOffset = output.position();
		}
		catch (IOException ex)
		{
			throw new UncheckedIOException(ex);
		}
		this.currOffset = new AtomicLong(startingOffset);
		this.chunkInfoList = Collections.synchronizedList(new ArrayList<>());
	}

	@Override
	public void writeValues(ChunkValuesWritingFunction writer) throws IOException
	{
		ColumnChunkPages pages = writer.apply(columnChunkWriter);
		long totalBytes = pages.totalBytesForStorage();
		long startingOffset = this.currOffset.getAndAdd(totalBytes);
		pages.writeToOutputStream(output, startingOffset);
		validateNumRows(pages);
		chunkInfoList.add(new PartialColumnChunkInfo(pages, computeStartingOffset(startingOffset), compressionCodec));
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
		if (!new HashSet<>(messageType.getColumns()).equals(descriptors))
		{
			throw new IllegalStateException("Not all columns in this row group were written. " + descriptors);
		}

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
