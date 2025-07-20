package com.earnix.parquet.columnar.writer;

import com.earnix.parquet.columnar.utils.ParquetEnumUtils;
import com.earnix.parquet.columnar.utils.ParquetMagicUtils;
import com.earnix.parquet.columnar.writer.rowgroup.ColumnChunkInfo;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupInfo;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
	 * @param messageType   the schema info
	 * @param rowGroupInfos the info on the row groups
	 * @return the built file metadata
	 */
	public static FileMetaData getFileMetaData(MessageType messageType, List<RowGroupInfo> rowGroupInfos)
	{
		List<SchemaElement> schemaElementList = ParquetWriterUtils.getSchemaElements(messageType);
		// C++ parquet driver requires row group columns to be in the same order as the schema.
		Map<ColumnDescriptor, Integer> schemaOrder = computeOrderingFromSchema(messageType);


		if (rowGroupInfos.isEmpty())
			throw new IllegalStateException("No groups written");

		FileMetaData fileMetaData = new FileMetaData();
		fileMetaData.setSchema(schemaElementList);

		long totalNumRows = rowGroupInfos.stream().mapToLong(RowGroupInfo::getNumRows).sum();
		fileMetaData.setNum_rows(totalNumRows);
		// TODO: what version are we actually??
		fileMetaData.setVersion(1);
		fileMetaData.setKey_value_metadata(new ArrayList<>());
		fileMetaData.getKey_value_metadata()
				.add(new KeyValue("original.created.by").setValue("ColumnarParquetTool Alpha"));

		fileMetaData.setRow_groups(getRowGroupList(schemaOrder, rowGroupInfos));
		return fileMetaData;
	}

	private static Map<ColumnDescriptor, Integer> computeOrderingFromSchema(MessageType messageType)
	{
		Map<ColumnDescriptor, Integer> schemaOrder = new HashMap<>();
		int idx = 0;
		for (ColumnDescriptor descriptor : messageType.getColumns())
		{
			schemaOrder.put(descriptor, idx++);
		}
		return schemaOrder;
	}

	private static List<RowGroup> getRowGroupList(Map<ColumnDescriptor, Integer> schemaOrder,
			List<RowGroupInfo> rowGroupInfos)
	{
		return rowGroupInfos.stream().map(rgi -> ParquetWriterUtils.getRowGroup(schemaOrder, rgi))
				.collect(Collectors.toList());
	}

	private static RowGroup getRowGroup(Map<ColumnDescriptor, Integer> schemaOrder, RowGroupInfo rowGroupInfo)
	{
		RowGroup rowGroup = new RowGroup();
		rowGroup.setFile_offset(rowGroupInfo.getStartingOffset());
		rowGroup.setNum_rows(rowGroupInfo.getNumRows());
		rowGroup.setColumns(getChunks(schemaOrder, rowGroupInfo));
		rowGroup.setTotal_compressed_size(rowGroupInfo.getCompressedSize());
		rowGroup.setTotal_byte_size(rowGroupInfo.getUncompressedSize());
		return rowGroup;
	}

	private static List<ColumnChunk> getChunks(Map<ColumnDescriptor, Integer> schemaOrder, RowGroupInfo rowGroupInfo)
	{
		ColumnChunk[] ret = new ColumnChunk[schemaOrder.size()];
		for (ColumnChunkInfo info : rowGroupInfo.getCols())
		{
			ret[schemaOrder.get(info.getDescriptor())] = info.buildChunkFromInfo();
		}
		return Collections.unmodifiableList(Arrays.asList(ret));
	}

	public static void writeLittleEndianInt(OutputStream os, int byteCount) throws IOException
	{
		byte[] toWrite = new byte[Integer.BYTES];
		ByteBuffer.wrap(toWrite).order(ByteOrder.LITTLE_ENDIAN).putInt(byteCount);
		os.write(toWrite);
	}

	private static List<SchemaElement> getSchemaElements(MessageType messageType)
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
