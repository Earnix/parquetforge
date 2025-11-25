package com.earnix.parquet.columnar.reader;

import com.earnix.parquet.columnar.reader.processors.ParquetColumnarProcessors;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;

public interface ParquetColumnarReader extends BaseColumnarReader
{
	void processFile(ParquetColumnarProcessors.RowGroupProcessor processor);

	void processFile(ParquetColumnarProcessors.ChunkProcessor processor);

	void processFile(ParquetColumnarProcessors.ProcessRawChunkBytes processor);

	FileMetaData readMetaData() throws IOException;
}
