package com.earnix.parquet.columnar.writer;

import com.earnix.parquet.columnar.writer.rowgroup.RowGroupInfo;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

/**
 * an abstract class containing fields comment to ParquetWriters.
 */
public abstract class BaseParquetColumnarWriter implements ParquetColumnarWriter
{
	private static final Set<Type.Repetition> supportedRepetition = EnumSet.of(REQUIRED, OPTIONAL);

	// may be lazily populated
	private MessageType messageType;

	protected final ParquetProperties parquetProperties;
	protected final CompressionCodec compressionCodec;
	protected final List<RowGroupInfo> rowGroupInfos = new ArrayList<>();
	private final List<KeyValue> keyValues = new ArrayList<>();

	public BaseParquetColumnarWriter(MessageType messageType, ParquetProperties parquetProperties,
			CompressionCodec compressionCodec)
	{
		if (messageType != null)
			setMessageType(messageType);
		this.parquetProperties = parquetProperties;
		this.compressionCodec = compressionCodec;
	}

	private static void validateMessageType(MessageType messageType)
	{
		for (ColumnDescriptor desc : messageType.getColumns())
		{
			validateDescriptor(desc);
		}
	}

	private static void validateDescriptor(ColumnDescriptor desc)
	{
		if (!supportedRepetition.contains(desc.getPrimitiveType().getRepetition()))
			throw new IllegalStateException("Not supported repetition type: " + desc);

		if (desc.getPath().length > 1)
			throw new IllegalStateException("Nesting not supported : " + desc);
	}

	protected MessageType getMessageType()
	{
		return messageType;
	}

	protected void setMessageType(MessageType messageType)
	{
		if (this.messageType != null)
			throw new IllegalStateException("Message type already set.");
		validateMessageType(messageType);
		this.messageType = messageType;
	}

	protected FileMetaData buildFileMetadata()
	{
		if (rowGroupInfos.isEmpty())
			throw new IllegalStateException("cannot build parquet file without row groups");
		return ParquetWriterUtils.getFileMetaData(
				requireNonNull(getMessageType(), "MessageType must be set before building FileMetaData"),
				rowGroupInfos, keyValues);
	}

	@Override
	public synchronized void addKeyValue(KeyValue keyValue)
	{
		this.keyValues.add(new KeyValue(keyValue));
	}
}
