package com.earnix.parquet.columnar.s3;

import com.earnix.parquet.columnar.writer.rowgroup.FileRowGroupWriterImpl;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;

import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * S3 supports multi part uploads, but parts must be at least 5MB except for the last part. Download performance depends
 * upon
 */
public class S3RowGroupWriterImpl extends FileRowGroupWriterImpl implements RowGroupWriter
{
	private final long offsetOfCurrentFile;

	public S3RowGroupWriterImpl(MessageType messageType, CompressionCodec compressionCodec,
			ParquetProperties parquetProperties, long numRows, Path outputFile, FileChannel output,
			long startingOffsetToWriteToInFile, long offsetOfCurrentFile)
	{
		super(messageType, compressionCodec, parquetProperties, numRows, outputFile, output,
				startingOffsetToWriteToInFile);
		this.offsetOfCurrentFile = offsetOfCurrentFile;
	}

	@Override
	protected long computeStartingOffset(long startingOffset)
	{
		return offsetOfCurrentFile + super.computeStartingOffset(startingOffset);
	}
}
