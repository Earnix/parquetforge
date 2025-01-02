package com.earnix.parquet.columnar.writer;

import com.earnix.parquet.columnar.utils.ParquetEnumUtils;
import com.earnix.parquet.columnar.utils.ParquetMagicUtils;
import com.earnix.parquet.columnar.writer.rowgroup.ColumnChunkInfo;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupInfo;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Util;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class for functions that assist in writing parquet files
 */
public class ParquetWriterUtils
{
	public static void writeFooterMetadataAndMagic(WritableByteChannel fileChannel, FileMetaData fileMetaData)
			throws IOException
	{
		CountingOutputStream os = new CountingOutputStream(Channels.newOutputStream(fileChannel));
		Util.writeFileMetaData(fileMetaData, os);
		int byteCount = Math.toIntExact(os.getByteCount());
		writeLittleEndianInt(os, byteCount);
		ParquetMagicUtils.writeMagicBytes(fileChannel);
	}

	/**
	 * Build Parquet Parquet Footer metadata
	 *
	 * @param rowGroupInfos     the info on the row groups
	 * @param schemaElementList the schema of the parquet file
	 * @return the built file metadata
	 */
	public static FileMetaData getFileMetaData(List<RowGroupInfo> rowGroupInfos, List<SchemaElement> schemaElementList)
	{
		if (rowGroupInfos.isEmpty())
			throw new IllegalStateException("No groups written");

		FileMetaData fileMetaData = new FileMetaData();
		fileMetaData.setSchema(schemaElementList);

		long totalNumRows = rowGroupInfos.stream().mapToLong(RowGroupInfo::getNumRows).sum();
		fileMetaData.setNum_rows(totalNumRows);
		// TODO: what version are we actually??
		fileMetaData.setVersion(1);

		fileMetaData.setRow_groups(getRowGroupList(rowGroupInfos));
		return fileMetaData;
	}

	private static List<RowGroup> getRowGroupList(List<RowGroupInfo> rowGroupInfos)
	{
		return rowGroupInfos.stream().map(ParquetWriterUtils::getRowGroup).collect(Collectors.toList());
	}

	private static RowGroup getRowGroup(RowGroupInfo rowGroupInfo)
	{
		RowGroup rowGroup = new RowGroup();
		rowGroup.setFile_offset(rowGroupInfo.getStartingOffset());
		rowGroup.setNum_rows(rowGroupInfo.getNumRows());
		rowGroup.setColumns(getChunks(rowGroupInfo));
		rowGroup.setTotal_compressed_size(rowGroupInfo.getCompressedSize());
		rowGroup.setTotal_byte_size(rowGroupInfo.getUncompressedSize());
		return rowGroup;
	}

	private static List<ColumnChunk> getChunks(RowGroupInfo rowGroupInfo)
	{
		return rowGroupInfo.getCols().stream().map(ColumnChunkInfo::buildChunkFromInfo).collect(Collectors.toList());
	}

	public static void writeLittleEndianInt(OutputStream os, int byteCount) throws IOException
	{
		byte[] toWrite = new byte[Integer.BYTES];
		ByteBuffer.wrap(toWrite).order(ByteOrder.LITTLE_ENDIAN).putInt(byteCount);
		os.write(toWrite);
	}

	public static List<SchemaElement> getSchemaElements(MessageType messageType)
	{
		List<SchemaElement> schemaElementList = new ArrayList<>(1 + messageType.getColumns().size());

		SchemaElement root = new SchemaElement();
		root.setName("root");
		root.setType(null);
		root.setNum_children(messageType.getColumns().size());
		schemaElementList.add(root);

		for (ColumnDescriptor descriptor : messageType.getColumns())
		{
			SchemaElement schemaElement = new SchemaElement();
			String colName = descriptor.getPath()[descriptor.getPath().length - 1];
			schemaElement.setName(colName);
			schemaElement.setType(ParquetEnumUtils.convert(descriptor.getPrimitiveType().getPrimitiveTypeName()));
			schemaElement.setRepetition_type(ParquetEnumUtils.convert(descriptor.getPrimitiveType().getRepetition()));
			schemaElementList.add(schemaElement);
		}
		return schemaElementList;
	}
}
