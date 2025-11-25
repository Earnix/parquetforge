package com.earnix.parquet.columnar.file.reader;

import com.earnix.parquet.columnar.RowGroupRowIndex;
import com.earnix.parquet.columnar.reader.ParquetColumnarReader;
import com.earnix.parquet.columnar.reader.ParquetMetadataUtils;
import com.earnix.parquet.columnar.reader.ParquetReaderUtils;
import com.earnix.parquet.columnar.reader.chunk.InMemRowGroup;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.reader.processors.ParquetColumnarProcessors;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.earnix.parquet.columnar.reader.ParquetReaderUtils.getLen;
import static com.earnix.parquet.columnar.reader.ParquetReaderUtils.getStartOffset;

/**
 * Read a parquet file column by column rather than row by row, and support different row processors as are defined in
 * {@link ParquetColumnarProcessors}
 */
public class ParquetColumnarFileReader implements ParquetColumnarReader
{
	private final Path parquetFilePath;
	private volatile FileMetaData metaData;
	private volatile MessageType messageType;
	private volatile List<ColumnDescriptor> columnDescriptors;
	private volatile RowGroupRowIndex rowGroupRowIndex;

	public ParquetColumnarFileReader(Path parquetFilePath)
	{
		this.parquetFilePath = parquetFilePath;
	}

	@Override
	public void processFile(ParquetColumnarProcessors.RowGroupProcessor processor)
	{
		processFile(processor, null, null);
	}

	@Override
	public void processFile(ParquetColumnarProcessors.ChunkProcessor processor)
	{
		processFile(null, processor, null);
	}

	@Override
	public void processFile(ParquetColumnarProcessors.ProcessRawChunkBytes processor)
	{
		processFile(null, null, processor);
	}

	private void processFile(ParquetColumnarProcessors.RowGroupProcessor rowGroupProcessor,
			ParquetColumnarProcessors.ChunkProcessor chunkProcessor,
			ParquetColumnarProcessors.ProcessRawChunkBytes chunkBytesProcessor)
	{
		try (FileChannel fc = FileChannel.open(parquetFilePath))
		{
			for (RowGroup rowGroup : readMetaData().getRow_groups())
			{
				Optional<List<InMemChunk>> optionalInMemChunksForRowGroupProcessing = processChunksOfRowGroup(
						chunkProcessor, chunkBytesProcessor, rowGroup, fc);
				processRowGroup(rowGroupProcessor, rowGroup, optionalInMemChunksForRowGroupProcessing);
			}
		}
		catch (IOException ex)
		{
			throw new UncheckedIOException(ex);
		}
	}

	private Optional<List<InMemChunk>> processChunksOfRowGroup(ParquetColumnarProcessors.ChunkProcessor chunkProcessor,
			ParquetColumnarProcessors.ProcessRawChunkBytes chunkBytesProcessor, RowGroup rowGroup, FileChannel fc)
	{
		boolean chunksNeededForRowGroupProcessing = chunksNeededForRowGroupProcessing(chunkProcessor,
				chunkBytesProcessor);

		List<InMemChunk> chunks = rowGroup.getColumns().stream()
				.map(columnChunk -> processChunkWithUncheckedException(chunkProcessor, chunkBytesProcessor, columnChunk,
						fc)).filter(inMemChunk -> chunksNeededForRowGroupProcessing && inMemChunk.isPresent())
				.map(Optional::get).collect(Collectors.toList());

		return chunksNeededForRowGroupProcessing ? Optional.of(chunks) : Optional.empty();
	}

	@Override
	public FileMetaData readMetaData() throws IOException
	{
		if (metaData == null)
		{
			synchronized (this)
			{
				if (metaData == null)
				{
					metaData = ParquetFileMetadataReader.readFileMetadata(parquetFilePath);
				}
			}
		}
		return metaData;
	}

	public Path getParquetFilePath()
	{
		return parquetFilePath;
	}

	private static InputStream getInputStream(FileChannel fc, ColumnChunk columnChunk) throws IOException
	{
		long startOffset = getStartOffset(columnChunk);
		fc.position(startOffset);
		return new BoundedInputStream(Channels.newInputStream(fc), getLen(columnChunk));
	}

	private static ColumnMetaData getAndValidateMetaData(ColumnChunk columnChunk) throws UnsupportedEncodingException
	{
		ColumnMetaData columnMetaData = columnChunk.getMeta_data();

		// we don't support getting chunks from other files yet.
		if (columnChunk.getFile_path() != null)
			throw new UnsupportedEncodingException();
		return columnMetaData;
	}

	private static void processRowGroup(ParquetColumnarProcessors.RowGroupProcessor rowGroupProcessor,
			RowGroup rowGroup, Optional<List<InMemChunk>> optionalInMemChunksForRowGroupProcessing)
	{
		if (rowGroupProcessor != null)
		{
			if (!optionalInMemChunksForRowGroupProcessing.isPresent())
			{
				throw new IllegalStateException("Chunk values list is empty in processing Row Group case");
			}

			rowGroupProcessor.processRowGroup(
					new InMemRowGroup(optionalInMemChunksForRowGroupProcessing.get(), rowGroup.getNum_rows()));
		}
	}

	private Optional<InMemChunk> processChunkWithUncheckedException(
			ParquetColumnarProcessors.ChunkProcessor chunkProcessor,
			ParquetColumnarProcessors.ProcessRawChunkBytes chunkBytesProcessor, ColumnChunk columnChunk, FileChannel fc)
	{
		try
		{
			return processChunk(chunkProcessor, chunkBytesProcessor, columnChunk, fc);
		}
		catch (IOException e)
		{
			throw new UncheckedIOException(e);
		}
	}

	private Optional<InMemChunk> processChunk(ParquetColumnarProcessors.ChunkProcessor chunkProcessor,
			ParquetColumnarProcessors.ProcessRawChunkBytes chunkBytesProcessor, ColumnChunk columnChunk, FileChannel fc)
			throws IOException
	{
		ColumnMetaData columnMetaData = getAndValidateMetaData(columnChunk);

		ColumnDescriptor descriptor = buildDescriptor(columnMetaData);
		InputStream is = getInputStream(fc, columnChunk);
		long chunkLen = columnMetaData.getTotal_compressed_size();
		CompressionCodec compressionCodec = columnMetaData.getCodec();

		processChunkBytes(chunkBytesProcessor, columnChunk, descriptor, is);
		processChunkValues(chunkProcessor, descriptor, is, chunkLen, compressionCodec);

		if (chunksNeededForRowGroupProcessing(chunkProcessor, chunkBytesProcessor))
		{
			return Optional.of(ParquetReaderUtils.readInMemChunk(descriptor, is, chunkLen, compressionCodec));
		}

		return Optional.empty();
	}

	private void processChunkValues(ParquetColumnarProcessors.ChunkProcessor chunkProcessor,
			ColumnDescriptor colDescriptor, InputStream is, long chunkLen, CompressionCodec compressionCodec)
			throws IOException
	{
		if (chunkProcessor != null)
		{
			chunkProcessor.processChunk(
					ParquetReaderUtils.readInMemChunk(colDescriptor, is, chunkLen, compressionCodec));
		}
	}

	@Override
	public int getNumRowGroups() throws IOException
	{
		return readMetaData().getRow_groupsSize();
	}

	@Override
	public long getNumRowsInRowGroup(int rowGroup) throws IOException
	{
		return readMetaData().getRow_groups().get(rowGroup).getNum_rows();
	}

	@Override
	public long getTotalNumRows() throws IOException
	{
		return readMetaData().getNum_rows();
	}

	@Override
	public RowGroupRowIndex getRowGroupRowIndex() throws IOException
	{
		return rgRowStart();
	}

	private RowGroupRowIndex rgRowStart()throws IOException
	{
		if (rowGroupRowIndex == null)
		{
			synchronized (this)
			{
				if (rowGroupRowIndex == null)
				{
					rowGroupRowIndex = new RowGroupRowIndex(readMetaData());
				}
			}
		}
		return rowGroupRowIndex;
	}

	@Override
	public MessageType getMessageType()
	{
		return messageType;
	}

	@Override
	public List<ColumnDescriptor> getColumnDescriptors() throws IOException
	{
		ensureDescriptorsInitialized();
		return columnDescriptors;
	}

	@Override
	public ColumnDescriptor getDescriptor(int colOffset) throws IOException
	{
		ensureDescriptorsInitialized();
		return columnDescriptors.get(colOffset);
	}

	protected void ensureDescriptorsInitialized() throws IOException
	{
		if (columnDescriptors == null)
		{
			synchronized (this)
			{
				if (columnDescriptors == null)
				{
					columnDescriptors = Collections.unmodifiableList(new ArrayList<>(buildMessageType().getColumns()));
				}
			}
		}
	}

	protected ColumnDescriptor buildDescriptor(ColumnMetaData columnMetaData) throws IOException
	{
		return buildMessageType().getColumnDescription(columnMetaData.getPath_in_schema().toArray(new String[0]));
	}

	protected MessageType buildMessageType() throws IOException
	{
		if (messageType == null)
		{
			synchronized (this)
			{
				if (messageType == null)
				{
					messageType = ParquetMetadataUtils.buildMessageType(readMetaData());
				}
			}
		}
		return messageType;
	}

	private void processChunkBytes(ParquetColumnarProcessors.ProcessRawChunkBytes chunkBytesProcessor,
			ColumnChunk columnChunk, ColumnDescriptor colDescriptor, InputStream is)
	{
		if (chunkBytesProcessor != null)
		{
			chunkBytesProcessor.processChunk(colDescriptor, columnChunk, is);
		}
	}

	private static boolean chunksNeededForRowGroupProcessing(ParquetColumnarProcessors.ChunkProcessor chunkProcessor,
			ParquetColumnarProcessors.ProcessRawChunkBytes chunkBytesProcessor)
	{
		return chunkProcessor == null && chunkBytesProcessor == null;
	}
}
