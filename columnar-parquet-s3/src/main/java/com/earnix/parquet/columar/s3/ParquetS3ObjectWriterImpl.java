package com.earnix.parquet.columar.s3;

import com.earnix.parquet.columnar.writer.ParquetColumnarWriter;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupWriter;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;

public class ParquetS3ObjectWriterImpl implements ParquetColumnarWriter
{
	private final S3Client s3Client;
	private final String bucket;
	private final String s3ObjectKey;

	// this needs to be a base class or wrapped elsewhere - dupe code in file writer
	private final MessageType messageType;
	private final ParquetProperties parquetProperties;
	private final CompressionCodec compressionCodec;

	private volatile boolean closed = false;

	public ParquetS3ObjectWriterImpl(CompressionCodec compressionCodec, ParquetProperties parquetProperties,
			MessageType messageType, String s3ObjectKey, String bucket, S3Client s3Client)
	{
		this.compressionCodec = compressionCodec;
		this.parquetProperties = parquetProperties;
		this.messageType = messageType;
		this.s3ObjectKey = s3ObjectKey;
		this.bucket = bucket;
		this.s3Client = s3Client;
	}

	@Override
	public RowGroupWriter startNewRowGroup(long numRows) throws IOException
	{
		throw new NotImplementedException();
	}

	@Override
	public void finishRowGroup() throws IOException
	{
		throw new NotImplementedException();
	}

	@Override
	public void finishAndWriteFooterMetadata() throws IOException
	{
		throw new NotImplementedException();
	}

	@Override
	public void close() throws IOException
	{
		throw new NotImplementedException();
	}
}
