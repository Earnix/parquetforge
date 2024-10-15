package com.earnix.parquet.columnar.writer.rowgroup;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;

import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkPages;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriter;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriterImpl;

public class FileRowGroupWriterImpl implements RowGroupWriter
{
	private final MessageType messageType;
	private final ColumnChunkWriter columnChunkWriter;
	private final FileChannel output;
	private final long numRows;
	private final long startingOffset;
	private final AtomicLong currOffset;
	private final List<ColumnChunkInfo> chunkInfo;
	private volatile boolean closed = false;

	public FileRowGroupWriterImpl(MessageType messageType, CompressionCodec compressionCodec,
			ParquetProperties parquetProperties, long numRows, FileChannel output) throws IOException
	{
		this.messageType = messageType;
		this.output = output;
		this.columnChunkWriter = new ColumnChunkWriterImpl(messageType, compressionCodec, parquetProperties, numRows);
		this.numRows = numRows;
		this.startingOffset = output.position();
		this.currOffset = new AtomicLong(startingOffset);
		this.chunkInfo = Collections.synchronizedList(new ArrayList<>());
	}

	@Override
	public void writeColumn(ChunkWriter writer) throws IOException
	{
		ColumnChunkPages pages = writer.apply(columnChunkWriter);
		long totalBytes = pages.totalBytesForStorage();
		long startingOffset = this.currOffset.getAndAdd(totalBytes);
		pages.writeToOutputStream(output, startingOffset);
		chunkInfo.add(new ColumnChunkInfo(pages.getColumnDescriptor(), pages.getEncodingSet(), pages.getNumValues(),
				startingOffset, totalBytes, pages.getUncompressedBytes()));
		assertNotClosed();
	}

	private void assertNotClosed()
	{
		if (closed)
			throw new IllegalStateException("Closed");
	}

	public RowGroupInfo closeAndValidateAllColumnsWritten() throws IOException
	{
		assertNotClosed();
		this.closed = true;
		Set<ColumnDescriptor> descriptors = chunkInfo.stream().map(ColumnChunkInfo::getDescriptor)
				.collect(Collectors.toSet());
		if (!new HashSet<>(messageType.getColumns()).equals(descriptors))
		{
			throw new IllegalStateException("Not all columns in this row group were written. " + descriptors);
		}

		this.output.position(this.currOffset.get());
		long len = this.currOffset.get() - this.startingOffset;
		return new RowGroupInfo(startingOffset, len, numRows, chunkInfo);
	}
}
