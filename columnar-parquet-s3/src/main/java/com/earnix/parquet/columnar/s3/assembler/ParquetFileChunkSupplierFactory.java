package com.earnix.parquet.columnar.s3.assembler;

import com.earnix.parquet.columnar.file.reader.ParquetFileMetadataReader;
import com.earnix.parquet.columnar.reader.ParquetMetadataUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ParquetFileChunkSupplierFactory
{
	private final Path parquetFilePath;
	private MessageType messageType;
	private FileMetaData fileMetaData;
	List<Map<ColumnDescriptor, ColumnChunk>> rowGroupToIndexedMetadata;

	public ParquetFileChunkSupplierFactory(Path parquetFilePath) throws IOException
	{
		this.parquetFilePath = parquetFilePath;
		try (FileChannel fc = FileChannel.open(parquetFilePath))
		{
			fileMetaData = ParquetFileMetadataReader.readMetadata(fc);
			messageType = ParquetMetadataUtils.buildMessageType(fileMetaData);
			rowGroupToIndexedMetadata = new ArrayList<>(fileMetaData.getRow_groupsSize());

			for (RowGroup rowGroup : fileMetaData.getRow_groups())
			{
				Map<ColumnDescriptor, ColumnChunk> columnDescriptorColumnMetaDataMap = new HashMap<>();
				Iterator<ColumnChunk> it = rowGroup.getColumnsIterator();
				while (it.hasNext())
				{
					ColumnChunk columnChunk = it.next();
					columnChunk.getMeta_data().getPath_in_schema();
				}
			}
		}
	}
}
