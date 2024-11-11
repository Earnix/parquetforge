package com.earnix.parquet.columnar.writer;

import com.earnix.parquet.columnar.utils.ParquetMagicUtils;
import com.earnix.parquet.columnar.writer.rowgroup.FileRowGroupWriterImpl;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupInfo;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public class ParquetFileColumnarWriterImpl implements ParquetColumnarWriter, Closeable
{
	private static final Set<Type.Repetition> supportedRepetition = EnumSet.of(REQUIRED, OPTIONAL);

	private final MessageType messageType;
	private final ParquetProperties parquetProperties;
	private final CompressionCodec compressionCodec;

	private FileRowGroupWriterImpl lastWriter = null;
	private final List<RowGroupInfo> rowGroupInfos = new ArrayList<>();
	private final FileChannel fileChannel;

	public ParquetFileColumnarWriterImpl(Path outputFile, Collection<PrimitiveType> primitiveTypeList)
			throws IOException
	{
		this(outputFile, primitiveTypeList, CompressionCodec.ZSTD);
	}

	/**
	 * Constructor for flat file.
	 *
	 * @param primitiveTypeList the types of columns
	 */
	public ParquetFileColumnarWriterImpl(Path outputFile, Collection<PrimitiveType> primitiveTypeList,
			CompressionCodec compressionCodec) throws IOException
	{
		this.compressionCodec = compressionCodec;
		for (PrimitiveType type : primitiveTypeList)
		{
			if (!supportedRepetition.contains(type.getRepetition()))
				throw new IllegalStateException("Not supported repetition type: " + type);
		}
		messageType = new MessageType("root", primitiveTypeList.toArray(new Type[0]));

		// this probably should be a constructor param
		parquetProperties = ParquetProperties.builder().build();
		fileChannel = FileChannel.open(outputFile, //
				StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
	}

	@Override
	public void writeRowGroup(long numRows, RowGroupAppender rowGroupAppender) throws IOException
	{
		RowGroupWriter rowGroupWriter = startNewRowGroup(numRows);
		rowGroupAppender.append(rowGroupWriter);
		finishRowGroup();
	}

	private RowGroupWriter startNewRowGroup(long numRows) throws IOException
	{
		if (lastWriter != null)
		{
			throw new IllegalStateException("Last writer was not closed");
		}
		if (rowGroupInfos.isEmpty())
		{
			ParquetMagicUtils.writeMagicBytes(fileChannel);
		}
		lastWriter = new FileRowGroupWriterImpl(messageType, compressionCodec, parquetProperties, numRows, fileChannel);
		return lastWriter;
	}

	private void finishRowGroup()
	{
		try
		{
			rowGroupInfos.add(lastWriter.closeAndValidateAllColumnsWritten());
			lastWriter = null;
		}
		catch (IOException ex)
		{
			throw new UncheckedIOException(ex);
		}
	}

	@Override
	public ParquetFileInfo finishAndWriteFooterMetadata() throws IOException
	{
		if (lastWriter != null)
			throw new IllegalStateException("Last writer was not closed");

		List<SchemaElement> schemaElements = ParquetWriterUtils.getSchemaElements(messageType);
		FileMetaData fileMetaData = ParquetWriterUtils.getFileMetaData(rowGroupInfos, schemaElements);
		ParquetWriterUtils.writeFooterMetadata(fileChannel, fileMetaData);

		return new ParquetFileInfo(fileChannel.position(), messageType, fileMetaData);
	}

	@Override
	public void close() throws IOException
	{
		fileChannel.close();
	}
}
