package com.earnix.parquet.columnar.reader;

import com.earnix.parquet.columnar.reader.chunk.internal.ChunkDecompressToPageStoreFactory;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunkPageStore;
import com.earnix.parquet.columnar.reader.processors.ParquetFileProcessors;
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
import java.io.UnsupportedEncodingException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ParquetColumarFileReader
{
	private static final String STRUCTURED_FILES_UNSUPPORTED = "Structured files are not yet supported";

	private final Path parquetFilePath;
	private volatile FileMetaData metaData;

	public ParquetColumarFileReader(Path parquetFilePath)
	{
		this.parquetFilePath = parquetFilePath;
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

	public void processFile(ParquetFileProcessors.ProcessPerChunk processor) throws IOException
	{
		FileMetaData md = getMetaData();

		Map<String, ColumnDescriptor> columnDescriptorMap = new HashMap<>();

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
					|| schemaElement.getRepetition_type() != FieldRepetitionType.REQUIRED)
			{
				throw new UnsupportedEncodingException(
						"Field: " + nameKey + " Unsupported: " + schemaElement.getRepetition_type());
			}
			PrimitiveType.PrimitiveTypeName primitiveTypeName = ParquetEnumUtils.convert(schemaElement.getType());

			PrimitiveType primitiveType = new PrimitiveType(
					ParquetEnumUtils.convert(schemaElement.getRepetition_type()), primitiveTypeName, nameKey);

			primitiveTypeList.add(primitiveType);
		}
		MessageType messageType = new MessageType(root.getName(), primitiveTypeList);

		try (FileChannel fc = FileChannel.open(parquetFilePath))
		{
			for (RowGroup rowGroup : md.getRow_groups())
			{
				for (ColumnChunk columnChunk : rowGroup.getColumns())
				{
					ColumnMetaData columnMetaData = columnChunk.getMeta_data();

					// we don't support getting chunks from other files yet.
					if (columnChunk.getFile_path() != null)
						throw new UnsupportedEncodingException();
					long startOffset = columnChunk.getFile_offset() + columnMetaData.getData_page_offset();
					long chunkLen = columnMetaData.getTotal_uncompressed_size();


					fc.position(startOffset);
					InputStream is = new BoundedInputStream(Channels.newInputStream(fc), chunkLen);
					ColumnDescriptor colDescriptor = messageType
							.getColumnDescription(columnMetaData.getPath_in_schema().toArray(new String[0]));
					CompressionCodec compressionCodec = columnMetaData.getCodec();
					InMemChunkPageStore inMemChunkPageStore = ChunkDecompressToPageStoreFactory
							.buildColumnChunkPageStore(colDescriptor, new CountingInputStream(is),
									columnMetaData.getTotal_compressed_size(), compressionCodec);
					processor.processChunk(new InMemChunk(inMemChunkPageStore));
				}
			}
		}
	}

	public void processFile(ParquetFileProcessors.ProcessPerRowGroup processor) throws IOException
	{

	}

	public void processFile(ParquetFileProcessors.ProcessRawChunkBytes processor) throws IOException
	{

	}
}
