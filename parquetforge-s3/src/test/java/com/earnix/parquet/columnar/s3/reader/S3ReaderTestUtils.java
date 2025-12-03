package com.earnix.parquet.columnar.s3.reader;

import com.earnix.parquet.columnar.s3.ParquetS3ObjectWriterImpl;
import com.earnix.parquet.columnar.s3.buffering.S3KeyUploader;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.util.List;

/**
 * Test helper for creating small parquet files on S3.
 */
final class S3ReaderTestUtils
{
	public static final String COLUMN_NAME = "testDouble";

	private S3ReaderTestUtils()
	{
	}

	static UploadedParquet uploadDoubleColumnParquet(S3Client s3Client, String bucket, String key,
			List<double[]> rowGroups) throws IOException
	{
		MessageType messageType = new MessageType("root",
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, COLUMN_NAME));
		ColumnDescriptor descriptor = messageType.getColumnDescription(new String[] { COLUMN_NAME });

		try (S3KeyUploader uploader = new S3KeyUploader(s3Client, bucket, key);
				ParquetColumnarWriter writer = new ParquetS3ObjectWriterImpl(messageType, CompressionCodec.ZSTD,
						ParquetProperties.builder().build(), uploader, 2))
		{
			for (double[] values : rowGroups)
			{
				writer.writeRowGroup(values.length, rowGroupWriter -> rowGroupWriter.writeValues(
						chunkWriter -> chunkWriter.writeColumn(descriptor, values)));
			}
			writer.finishAndWriteFooterMetadata();
		}

		return new UploadedParquet(messageType, rowGroups);
	}

	static final class UploadedParquet
	{
		final MessageType messageType;
		final List<double[]> rowGroups;

		private UploadedParquet(MessageType messageType, List<double[]> rowGroups)
		{
			this.messageType = messageType;
			this.rowGroups = rowGroups;
		}

		ColumnDescriptor descriptor()
		{
			return messageType.getColumns().get(0);
		}

		long totalRows()
		{
			return rowGroups.stream().mapToLong(group -> group.length).sum();
		}

		long numRowGroups()
		{
			return rowGroups.size();
		}

		long rowsInGroup(int idx)
		{
			return rowGroups.get(idx).length;
		}
	}
}

