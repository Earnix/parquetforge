package com.earnix.parquet.columnar.s3.reader;

import com.earnix.parquet.columnar.s3.buffering.S3KeyUploader;
import com.earnix.parquet.columnar.writer.columnchunk.NullableIterators.NullableDoubleIterator;
import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import com.earnix.parquet.columnar.file.writer.ParquetFileColumnarWriterFactory;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
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

		Path tmpFile = Files.createTempFile("s3-reader", ".parquet");
		try (ParquetColumnarWriter writer = ParquetFileColumnarWriterFactory.createWriter(tmpFile, messageType,
				CompressionCodec.ZSTD, true))
		{
			for (double[] values : rowGroups)
			{
				double[] copy = Arrays.copyOf(values, values.length);
				writer.writeRowGroup(copy.length, rowGroupWriter -> rowGroupWriter.writeValues(chunkWriter -> chunkWriter
						.writeColumn(descriptor, new ArrayBackedNullableDoubleIterator(copy))));
			}
			writer.finishAndWriteFooterMetadata();
		}

		byte[] fileBytes = Files.readAllBytes(tmpFile);
		try (S3KeyUploader uploader = new S3KeyUploader(s3Client, bucket, key))
		{
			uploader.uploadPart(1, fileBytes.length, () -> new ByteArrayInputStream(fileBytes));
			uploader.finish();
		}
		finally
		{
			Files.deleteIfExists(tmpFile);
		}
		return new UploadedParquet(messageType, descriptor, rowGroups);
	}

	static final class UploadedParquet
	{
		final MessageType messageType;
		final ColumnDescriptor descriptor;
		final List<double[]> rowGroups;

		private UploadedParquet(MessageType messageType, ColumnDescriptor descriptor, List<double[]> rowGroups)
		{
			this.messageType = messageType;
			this.descriptor = descriptor;
			this.rowGroups = rowGroups;
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

	private static final class ArrayBackedNullableDoubleIterator implements NullableDoubleIterator
	{
		private final double[] values;
		private int idx = -1;

		private ArrayBackedNullableDoubleIterator(double[] values)
		{
			this.values = values;
		}

		@Override
		public double getValue()
		{
			return values[idx];
		}

		@Override
		public boolean isNull()
		{
			return false;
		}

		@Override
		public boolean next()
		{
			idx++;
			return idx < values.length;
		}
	}
}

