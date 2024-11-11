package com.earnix.parquet.columnar.s3.assembler;

import org.apache.parquet.format.ColumnChunk;

import java.io.IOException;
import java.io.InputStream;

public interface ParquetColumnChunkSupplier
{
	ColumnChunk getColumnChunk() throws IOException;

	InputStream openInputStream() throws IOException;
}
