package com.earnix.parquet.columnar.file.writer;

import com.earnix.parquet.columnar.utils.ParquetMagicUtils;
import com.earnix.parquet.columnar.writer.BaseParquetColumnarWriter;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import com.earnix.parquet.columnar.writer.ParquetFileInfo;
import com.earnix.parquet.columnar.writer.ParquetWriterUtils;
import com.earnix.parquet.columnar.writer.rowgroup.ColumnChunkInfo;
import com.earnix.parquet.columnar.writer.rowgroup.FileRowGroupWriterImpl;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupInfo;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupWriter;
import org.apache.commons.io.function.IOConsumer;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class ParquetFileColumnarWriterImpl extends BaseParquetColumnarWriter implements ParquetColumnarWriter, Closeable
{
	private static final String TABLE_ROOT_NAME = "root";

	private FileRowGroupWriterImpl lastWriter = null;
	private final Path outputFile;
	private final FileChannel fileChannel;
	private long offsetInFile;

	ParquetFileColumnarWriterImpl(Path outputFile, MessageType messageType, CompressionCodec compressionCodec,
			boolean cacheFileChannel) throws IOException
	{
		super(messageType, ParquetProperties.builder().build(), compressionCodec);

		this.outputFile = outputFile;
		// need to configure whether we want to hold the open channel
		fileChannel = cacheFileChannel ? openCachedChannel() : null;

		// if we don't cache the file channel, open it and close it to check permissions and truncate any existing data
		if (fileChannel == null)
		{
			openCachedChannel().close();
		}
	}

	private FileChannel openCachedChannel() throws IOException
	{
		return FileChannel.open(outputFile, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE,
				StandardOpenOption.CREATE);
	}

	private FileChannel openChannel() throws IOException
	{
		return FileChannel.open(outputFile, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
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
			writeToFile(fc -> ParquetMagicUtils.writeMagicBytes(fc));
		}

		lastWriter = new FileRowGroupWriterImpl(getMessageType(), compressionCodec, parquetProperties, numRows,
				outputFile, fileChannel, offsetInFile);
		return lastWriter;
	}

	@Override
	public RowGroupWriter getCurrentRowGroupWriter()
	{
		return lastWriter;
	}

	private void writeToFile(IOConsumer<FileChannel> operation) throws IOException
	{
		if (fileChannel == null)
		{
			try (FileChannel fc = openChannel())
			{
				operation.accept(fc);
				offsetInFile = fc.position();
			}
		}
		else
		{
			operation.accept(fileChannel);
			offsetInFile = fileChannel.position();
		}
	}

	@Override
	public void finishRowGroup() throws IOException
	{
		RowGroupInfo rowGrpInfo = lastWriter.closeAndValidateAllColumnsWritten();
		offsetInFile += rowGrpInfo.getCompressedSize();
		boolean isFirstRowGrp = rowGroupInfos.isEmpty();

		if (isFirstRowGrp && getMessageType() == null)
		{
			generateSchemaFromFirstRowGroup(rowGrpInfo);
		}

		rowGroupInfos.add(rowGrpInfo);
		lastWriter = null;
	}

	private void generateSchemaFromFirstRowGroup(RowGroupInfo rowGrpInfo)
	{
		// lazily generate message schema.
		Type[] cols = rowGrpInfo.getCols().stream()//
				.map(ColumnChunkInfo::getDescriptor)//
				.map(ColumnDescriptor::getPrimitiveType)//
				.toArray(Type[]::new);
		setMessageType(new MessageType(TABLE_ROOT_NAME, cols));
	}

	@Override
	public ParquetFileInfo finishAndWriteFooterMetadata() throws IOException
	{
		if (lastWriter != null)
			throw new IllegalStateException("Last writer was not closed");

		long footerMetadataOffset = offsetInFile;
		FileMetaData fileMetaData = buildFileMetadata();
		writeToFile(fc -> ParquetWriterUtils.writeFooterMetadataAndMagic(fc, fileMetaData));

		return new ParquetFileInfo(footerMetadataOffset, offsetInFile, getMessageType(), fileMetaData);
	}

	@Override
	public void close() throws IOException
	{
		if (fileChannel != null)
			fileChannel.close();
	}
}
