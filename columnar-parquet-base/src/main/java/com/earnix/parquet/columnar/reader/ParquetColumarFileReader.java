package com.earnix.parquet.columnar.reader;

import com.earnix.parquet.columnar.reader.chunk.InMemRowGroup;
import com.earnix.parquet.columnar.reader.chunk.internal.ChunkDecompressToPageStoreFactory;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunkPageStore;
import com.earnix.parquet.columnar.reader.processors.ParquetColumnarProcessors;
import com.earnix.parquet.columnar.utils.ParquetEnumUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Read a parquet file column by column rather than row by row, and support different row processors as are defined in
 * {@link ParquetColumnarProcessors}
 */
public class ParquetColumarFileReader
{
	private static final String STRUCTURED_FILES_UNSUPPORTED = "Structured files are not yet supported";

	private final Path parquetFilePath;
	private volatile FileMetaData metaData;
	private volatile MessageType messageType;

	public ParquetColumarFileReader(Path parquetFilePath)
	{
		this.parquetFilePath = parquetFilePath;
	}


	public void processFile(ParquetColumnarProcessors.RowGroupProcessor processor)
	{
		processFile(processor, null, null);
	}

	public void processFile(ParquetColumnarProcessors.ChunkProcessor processor)
	{
		processFile(null, processor, null);
	}

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
				Optional<List<InMemChunk>> optionalInMemChunksForRowGroupProcessing = processChunksOfRowGroup(chunkProcessor, chunkBytesProcessor, rowGroup, fc);
				processRowGroup(rowGroupProcessor, rowGroup, optionalInMemChunksForRowGroupProcessing);
			}
		}
		catch (IOException ex)
		{
			throw new UncheckedIOException(ex);
		}
	}

	private Optional<List<InMemChunk>> processChunksOfRowGroup(ParquetColumnarProcessors.ChunkProcessor chunkProcessor, ParquetColumnarProcessors.ProcessRawChunkBytes chunkBytesProcessor,
			RowGroup rowGroup, FileChannel fc)
	{
		boolean chunksNeededForRowGroupProcessing = chunksNeededForRowGroupProcessing(chunkProcessor, chunkBytesProcessor);

		List<InMemChunk> chunks = rowGroup.getColumns().stream()
				.map(columnChunk -> processChunkWithUncheckedException(chunkProcessor, chunkBytesProcessor, columnChunk, fc))
				.filter(inMemChunk -> chunksNeededForRowGroupProcessing && inMemChunk.isPresent())
				.map(Optional::get)
				.collect(Collectors.toList());

		return chunksNeededForRowGroupProcessing ? Optional.of(chunks) : Optional.empty();
	}

	public FileMetaData readMetaData() throws IOException
	{
		if (metaData == null)
		{
			synchronized (this)
			{
				if (metaData == null)
				{
					try (FileChannel fc = FileChannel.open(parquetFilePath))
					{
						metaData = ParquetFileMetadataReader.readMetadata(fc);
					}
				}
			}
		}
		return metaData;
	}

	private static InputStream getInputStream(FileChannel fc, ColumnChunk columnChunk) throws IOException
	{
		ColumnMetaData columnMetaData = columnChunk.getMeta_data();
		long startOffset = columnChunk.getFile_offset() + columnMetaData.getData_page_offset();
		fc.position(startOffset);
		return new BoundedInputStream(Channels.newInputStream(fc), columnMetaData.getTotal_compressed_size());
	}

	private static ColumnMetaData getAndValidateMetaData(ColumnChunk columnChunk) throws UnsupportedEncodingException
	{
		ColumnMetaData columnMetaData = columnChunk.getMeta_data();

		// we don't support getting chunks from other files yet.
		if (columnChunk.getFile_path() != null)
			throw new UnsupportedEncodingException();
		return columnMetaData;
	}

	private static void processRowGroup(ParquetColumnarProcessors.RowGroupProcessor rowGroupProcessor, RowGroup rowGroup, Optional<List<InMemChunk>> optionalInMemChunksForRowGroupProcessing)
	{
		if (rowGroupProcessor != null)
		{
			if (!optionalInMemChunksForRowGroupProcessing.isPresent())
			{
				throw new IllegalStateException("Chunk values list is empty in processing Row Group case");
			}

			rowGroupProcessor.processRowGroup(new InMemRowGroup(optionalInMemChunksForRowGroupProcessing.get(), rowGroup.getNum_rows()));
		}
	}

	private Optional<InMemChunk> processChunkWithUncheckedException(ParquetColumnarProcessors.ChunkProcessor chunkProcessor,
			ParquetColumnarProcessors.ProcessRawChunkBytes chunkBytesProcessor,
			ColumnChunk columnChunk, FileChannel fc)
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
			ParquetColumnarProcessors.ProcessRawChunkBytes chunkBytesProcessor,
			ColumnChunk columnChunk, FileChannel fc) throws IOException
	{
		ColumnMetaData columnMetaData = getAndValidateMetaData(columnChunk);

		ColumnDescriptor descriptor = buildDescriptor(buildMessageType(), columnMetaData);
		InputStream is = getInputStream(fc, columnChunk);
		long chunkLen = columnMetaData.getTotal_compressed_size();
		CompressionCodec compressionCodec = columnMetaData.getCodec();

		processChunkBytes(chunkBytesProcessor, columnChunk, descriptor, is);
		processChunkValues(chunkProcessor, descriptor, is, chunkLen, compressionCodec);

		if (chunksNeededForRowGroupProcessing(chunkProcessor, chunkBytesProcessor))
		{
			return Optional.of(readInMemChunk(descriptor, is, chunkLen, compressionCodec));
		}

		return Optional.empty();
	}

	private void processChunkValues(ParquetColumnarProcessors.ChunkProcessor chunkProcessor,
			ColumnDescriptor colDescriptor, InputStream is, long chunkLen, CompressionCodec compressionCodec)
			throws IOException
	{
		if (chunkProcessor != null)
		{
			chunkProcessor.processChunk(readInMemChunk( colDescriptor, is, chunkLen, compressionCodec));
		}
	}

	private InMemChunk readInMemChunk(ColumnDescriptor colDescriptor, InputStream is, long chunkLen, CompressionCodec compressionCodec) throws IOException
	{
		InMemChunkPageStore inMemChunkPageStore = ChunkDecompressToPageStoreFactory.buildColumnChunkPageStore(colDescriptor, new CountingInputStream(is), chunkLen, compressionCodec);
		return new InMemChunk(inMemChunkPageStore);
	}


	private ColumnDescriptor buildDescriptor(MessageType messageType, ColumnMetaData columnMetaData)
	{
		return messageType.getColumnDescription(columnMetaData.getPath_in_schema().toArray(new String[0]));
	}

	private MessageType buildMessageType() throws IOException
	{
		if (messageType == null)
		{
			synchronized (this)
			{
				if (messageType == null)
				{
					messageType = buildMessageType(readMetaData());
				}
			}
		}
		return messageType;
	}

	private void processChunkBytes(ParquetColumnarProcessors.ProcessRawChunkBytes chunkBytesProcessor, ColumnChunk columnChunk, ColumnDescriptor colDescriptor, InputStream is)
	{
		if (chunkBytesProcessor != null)
		{
			chunkBytesProcessor.processChunk(colDescriptor, columnChunk, is);
		}
	}

	private static MessageType buildMessageType(FileMetaData md) throws UnsupportedEncodingException
	{
		Iterator<SchemaElement> it = md.getSchemaIterator();
		SchemaElement root = it.next();
		if (root.getNum_children() + 1 != md.getSchemaSize())
		{
			throw new UnsupportedEncodingException(STRUCTURED_FILES_UNSUPPORTED);
		}

		List<Type> primitiveTypeList = new ArrayList<>(root.getNum_children());
		while (it.hasNext())
		{
			SchemaElement schemaElement = it.next();
			String nameKey = schemaElement.getName();
			if (schemaElement.getRepetition_type() != FieldRepetitionType.OPTIONAL
					&& schemaElement.getRepetition_type() != FieldRepetitionType.REQUIRED)
			{
				throw new UnsupportedEncodingException(
						"Field: " + nameKey + " Unsupported: " + schemaElement.getRepetition_type());
			}
			PrimitiveType.PrimitiveTypeName primitiveTypeName = ParquetEnumUtils.convert(schemaElement.getType());

			PrimitiveType primitiveType = new PrimitiveType(
					ParquetEnumUtils.convert(schemaElement.getRepetition_type()), primitiveTypeName, nameKey);

			primitiveTypeList.add(primitiveType);
		}
		return new MessageType(root.getName(), primitiveTypeList);
	}

	private static boolean chunksNeededForRowGroupProcessing(ParquetColumnarProcessors.ChunkProcessor chunkProcessor, ParquetColumnarProcessors.ProcessRawChunkBytes chunkBytesProcessor)
	{
		return chunkProcessor == null && chunkBytesProcessor == null;
	}

}
