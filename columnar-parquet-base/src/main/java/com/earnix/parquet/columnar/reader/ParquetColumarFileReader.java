package com.earnix.parquet.columnar.reader;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.earnix.parquet.columnar.reader.chunk.InMemRowGroup;
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

import com.earnix.parquet.columnar.reader.chunk.internal.ChunkDecompressToPageStoreFactory;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunkPageStore;
import com.earnix.parquet.columnar.reader.processors.ParquetColumnarProcessors;
import com.earnix.parquet.columnar.utils.ParquetEnumUtils;

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
			for (RowGroup rowGroup : getMetaData().getRow_groups())
			{
				Map<ColumnDescriptor, InMemChunk> inMemChunkMap = rowGroupProcessor == null ? null : new HashMap<>();

				for (ColumnChunk columnChunk : rowGroup.getColumns())
				{
					processChunk(chunkProcessor, chunkBytesProcessor, columnChunk, inMemChunkMap, fc);
				}

				processRowGroup(rowGroupProcessor, rowGroup, inMemChunkMap);
			}
		}
		catch (IOException ex)
		{
			throw new UncheckedIOException(ex);
		}
	}

	public FileMetaData getMetaData() throws IOException
	{
		readMetadata();
		return metaData;
	}

	private void readMetadata() throws IOException
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
	}

	private static InputStream getInputStream(FileChannel fc, ColumnChunk columnChunk) throws IOException
	{
		ColumnMetaData columnMetaData = columnChunk.getMeta_data();
		long startOffset = columnChunk.getFile_offset() + columnMetaData.getData_page_offset();
		fc.position(startOffset);
		return new BoundedInputStream(Channels.newInputStream(fc), columnMetaData.getTotal_compressed_size());
	}

	private static ColumnMetaData getValidMetaData(ColumnChunk columnChunk) throws UnsupportedEncodingException
	{
		ColumnMetaData columnMetaData = columnChunk.getMeta_data();

		// we don't support getting chunks from other files yet.
		if (columnChunk.getFile_path() != null)
			throw new UnsupportedEncodingException();
		return columnMetaData;
	}

	private static void processRowGroup(ParquetColumnarProcessors.RowGroupProcessor rowGroupProcessor, RowGroup rowGroup, Map<ColumnDescriptor, InMemChunk> inMemChunkMap)
	{
		if (rowGroupProcessor != null)
		{
			rowGroupProcessor.processRowGroup(new InMemRowGroup(Objects.requireNonNull(inMemChunkMap), rowGroup.getNum_rows()));
		}
	}

	private void processChunk(ParquetColumnarProcessors.ChunkProcessor chunkProcessor,
			ParquetColumnarProcessors.ProcessRawChunkBytes chunkBytesProcessor,
			ColumnChunk columnChunk, Map<ColumnDescriptor, InMemChunk> inMemChunkMap, FileChannel fc) throws IOException
	{
		ColumnMetaData columnMetaData = getValidMetaData(columnChunk);

		ColumnDescriptor descriptor = getDescriptor(columnMetaData);
		InputStream is = getInputStream(fc, columnChunk);
		long chunkLen = columnMetaData.getTotal_compressed_size();
		CompressionCodec compressionCodec = columnMetaData.getCodec();

		processChunkValues(chunkProcessor, inMemChunkMap, descriptor, is, chunkLen, compressionCodec);
		processChunkBytes(chunkBytesProcessor, columnChunk, descriptor, is);
	}

	private void processChunkValues(ParquetColumnarProcessors.ChunkProcessor chunkProcessor,
			Map<ColumnDescriptor, InMemChunk> inMemChunkMap,
			ColumnDescriptor colDescriptor, InputStream is, long chunkLen, CompressionCodec compressionCodec)
			throws IOException
	{
		if (chunkProcessor != null || inMemChunkMap != null)
		{
			InMemChunkPageStore inMemChunkPageStore = ChunkDecompressToPageStoreFactory.buildColumnChunkPageStore(colDescriptor, new CountingInputStream(is), chunkLen, compressionCodec);

			InMemChunk inMemChunk = new InMemChunk(inMemChunkPageStore);
			if (chunkProcessor != null)
			{
				chunkProcessor.processChunk(inMemChunk);
			}

			if (inMemChunkMap != null)
			{
				inMemChunkMap.put(colDescriptor, inMemChunk);
			}
		}
	}

	private ColumnDescriptor getDescriptor(ColumnMetaData columnMetaData) throws IOException
	{
		return getMessageType().getColumnDescription(columnMetaData.getPath_in_schema().toArray(new String[0]));
	}

	private MessageType getMessageType() throws IOException
	{
		if (messageType == null)
		{
			synchronized (this)
			{
				if (messageType == null)
				{
					messageType = buildMessageType(getMetaData());
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

}
