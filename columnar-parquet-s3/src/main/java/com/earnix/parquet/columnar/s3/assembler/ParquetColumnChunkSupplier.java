package com.earnix.parquet.columnar.s3.assembler;

import org.apache.parquet.format.ColumnChunk;

import java.io.IOException;
import java.io.InputStream;

/**
 * Represents all of the information to put this column chunk within a parquet file
 */
public interface ParquetColumnChunkSupplier
{
	long getNumRows();

	long getCompressedLength();

	ColumnChunk getColumnChunk() throws IOException;

	InputStream openInputStream() throws IOException;
}
