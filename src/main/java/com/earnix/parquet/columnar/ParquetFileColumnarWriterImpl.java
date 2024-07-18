package com.earnix.parquet.columnar;

import com.earnix.parquet.columnar.rowgroup.ColumnChunkInfo;
import com.earnix.parquet.columnar.rowgroup.FileRowGroupWriterImpl;
import com.earnix.parquet.columnar.rowgroup.RowGroupInfo;
import com.earnix.parquet.columnar.rowgroup.RowGroupWriter;
import com.github.luben.zstd.Zstd;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Util;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public class ParquetFileColumnarWriterImpl implements ParquetColumnarWriter, Closeable
{
	private static final Set<Type.Repetition> supportedRepetition = EnumSet.of(REQUIRED, OPTIONAL);
	private static final byte[] magicBytes = "PAR1".getBytes(StandardCharsets.US_ASCII);

	private final MessageType messageType;
	private final ParquetProperties parquetProperties;
	private final FileChannel fileChannel;
	private final CompressionCodec compressionCodec;
	private FileRowGroupWriterImpl lastWriter = null;
	private final List<RowGroupInfo> rowGroupInfos = new ArrayList<>();

	public ParquetFileColumnarWriterImpl(Path outputFile, List<PrimitiveType> primitiveTypeList) throws IOException
	{
		this(outputFile, primitiveTypeList, CompressionCodec.ZSTD);
	}

	/**
	 * Constructor for flat file.
	 * 
	 * @param primitiveTypeList the types of columns
	 */
	ParquetFileColumnarWriterImpl(Path outputFile, List<PrimitiveType> primitiveTypeList,
			CompressionCodec compressionCodec) throws IOException
	{
		this.compressionCodec = compressionCodec;
		for (PrimitiveType type : primitiveTypeList)
		{
			if (!supportedRepetition.contains(type.getRepetition()))
				throw new IllegalStateException("Only required is supported for now " + type);
		}
		messageType = new MessageType("root", primitiveTypeList.toArray(new Type[0]));

		// this probably should be a constructor param
		parquetProperties = ParquetProperties.builder().build();
		fileChannel = FileChannel.open(outputFile, //
				StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
	}

	@Override
	public RowGroupWriter startNewRowGroup(long numRows) throws IOException
	{
		if (lastWriter != null)
		{
			throw new IllegalStateException("Last writer was not closed");
		}
		if (rowGroupInfos.isEmpty())
		{
			writeMagicBytes();
		}
		lastWriter = new FileRowGroupWriterImpl(messageType, compressionCodec, parquetProperties, numRows, fileChannel);
		return lastWriter;
	}

	public void finishRowGroup() throws IOException
	{
		this.rowGroupInfos.add(lastWriter.closeAndValidateAllColumnsWritten());
		lastWriter = null;
	}

	private void writeMagicBytes() throws IOException
	{
		int bytesWritten = fileChannel.write(ByteBuffer.wrap(magicBytes));
		if (bytesWritten != magicBytes.length)
			throw new IllegalStateException();
	}

	@Override
	public void finishAndWriteFooterMetadata() throws IOException
	{
		if (lastWriter != null)
			throw new IllegalStateException("Last writer was not closed");
		if (rowGroupInfos.isEmpty())
			throw new IllegalStateException("No groups written");

		FileMetaData fileMetaData = new FileMetaData();

		List<SchemaElement> schemaElementList = getSchemaElements();
		fileMetaData.setSchema(schemaElementList);

		long totalNumRows = this.rowGroupInfos.stream().mapToLong(RowGroupInfo::getNumRows).sum();
		fileMetaData.setNum_rows(totalNumRows);
		// TODO: what version are we actually??
		fileMetaData.setVersion(1);

		List<RowGroup> rowGroups = new ArrayList<>(rowGroupInfos.size());
		for (RowGroupInfo rowGroupInfo : rowGroupInfos)
		{
			RowGroup rowGroup = new RowGroup();
			rowGroup.setNum_rows(rowGroupInfo.getNumRows());

			List<ColumnChunk> chunks = new ArrayList<>(rowGroupInfo.getCols().size());
			for (ColumnChunkInfo chunkInfo : rowGroupInfo.getCols())
			{
				ColumnChunk columnChunk = new ColumnChunk();
				columnChunk.setFile_offset(0);

				ColumnMetaData columnMetaData = new ColumnMetaData();
				columnMetaData.setData_page_offset(chunkInfo.getStartPos());
				columnMetaData.setTotal_compressed_size(chunkInfo.getCompressedLen());
				columnMetaData.setTotal_uncompressed_size(chunkInfo.getUncompressedLen());

				columnMetaData.setNum_values(chunkInfo.getNumValues());

				columnMetaData.setPath_in_schema(Arrays.asList(chunkInfo.getDescriptor().getPath()));
				columnMetaData.setType(convert(chunkInfo.getDescriptor().getPrimitiveType().getPrimitiveTypeName()));

				// the set of all encodings
				columnMetaData.setEncodings(new ArrayList<>(chunkInfo.getUsedEncodings()));

				columnMetaData.setCodec(compressionCodec);
				columnChunk.setMeta_data(columnMetaData);
				chunks.add(columnChunk);
			}
			rowGroup.setColumns(chunks);
			rowGroup.setTotal_compressed_size(rowGroupInfo.getCompressedSize());
			rowGroup.setTotal_byte_size(rowGroupInfo.getUncompressedSize());
			rowGroups.add(rowGroup);
		}
		fileMetaData.setRow_groups(rowGroups);

		CountingOutputStream os = new CountingOutputStream(Channels.newOutputStream(fileChannel));
		Util.writeFileMetaData(fileMetaData, os);
		os.getByteCount();
		byte[] toWrite = new byte[Integer.BYTES];
		ByteBuffer.wrap(toWrite).order(ByteOrder.LITTLE_ENDIAN).putInt(Math.toIntExact(os.getCount()));
		os.write(toWrite);
		writeMagicBytes();
	}

	private List<SchemaElement> getSchemaElements()
	{
		List<SchemaElement> schemaElementList = new ArrayList<>(1 + this.messageType.getColumns().size());

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
			schemaElement.setType(convert(descriptor.getPrimitiveType().getPrimitiveTypeName()));
			schemaElement.setRepetition_type(convert(descriptor.getPrimitiveType().getRepetition()));
			schemaElementList.add(schemaElement);
		}
		return schemaElementList;
	}

	private static Encoding convert(org.apache.parquet.column.Encoding encoding)
	{
		return Encoding.valueOf(encoding.name());
	}

	private static org.apache.parquet.format.Type convert(PrimitiveType.PrimitiveTypeName primitiveTypeName)
	{
		if (PrimitiveType.PrimitiveTypeName.BINARY.equals(primitiveTypeName))
		{
			return org.apache.parquet.format.Type.BYTE_ARRAY;
		}
		return org.apache.parquet.format.Type.valueOf(primitiveTypeName.name());
	}

	private static FieldRepetitionType convert(Type.Repetition repetition)
	{
		return FieldRepetitionType.valueOf(repetition.name());
	}

	private void finishLastRowGroup() throws IOException
	{
		rowGroupInfos.add(lastWriter.closeAndValidateAllColumnsWritten());
		lastWriter = null;
	}

	@Override
	public void close() throws IOException
	{
		fileChannel.close();
	}
}
