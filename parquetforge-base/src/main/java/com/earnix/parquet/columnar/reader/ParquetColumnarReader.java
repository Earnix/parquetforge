package com.earnix.parquet.columnar.reader;

import com.earnix.parquet.columnar.reader.processors.ParquetColumnarProcessors;
import org.apache.parquet.format.FileMetaData;

import java.io.IOException;

public interface ParquetColumnarReader extends BaseColumnarReader
{
	void processFile(ParquetColumnarProcessors.RowGroupProcessor processor);

	void processFile(ParquetColumnarProcessors.ChunkProcessor processor);

	void processFile(ParquetColumnarProcessors.ProcessRawChunkBytes processor);

	FileMetaData readMetaData() throws IOException;
}
