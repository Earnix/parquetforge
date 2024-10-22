package com.earnix.parquet.columar.s3;

import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkPages;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriter;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkWriterImpl;
import com.earnix.parquet.columnar.writer.rowgroup.ChunkWriter;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * S3 supports multi part uploads, but parts must be at least 5MB except for the last part. Download performance depends
 * upon
 */
public class S3RowGroupWriterImpl implements RowGroupWriter
{
	private final ColumnChunkWriter columnChunkWriter;
	private final List<ColumnChunkPages> bufferedPages;
	private final long minS3PartSize;
	private final long bufferedPage = 0;

	public S3RowGroupWriterImpl(MessageType messageType, CompressionCodec compressionCodec,
			ParquetProperties parquetProperties, long numRows, long targetMinPartSize)
	{
		this.columnChunkWriter = new ColumnChunkWriterImpl(messageType, compressionCodec, parquetProperties, numRows);
		bufferedPages = Collections.synchronizedList(new ArrayList<>());
		minS3PartSize = Math.max(S3Constants.MIN_S3_PART_SIZE, targetMinPartSize);
	}

	@Override
	public void writeColumn(ChunkWriter writer) throws IOException
	{
		ColumnChunkPages pages = writer.apply(columnChunkWriter);
		// write this to a file.
	}
}
