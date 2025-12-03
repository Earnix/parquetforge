package com.earnix.parquet.columnar.reader;

import com.earnix.parquet.columnar.reader.processors.ParquetColumnarProcessors;
import org.apache.parquet.format.FileMetaData;

import java.io.IOException;

public interface ParquetColumnarReader extends BaseColumnarReader
{
	/**
	 * Process the entire parquet file, rowgroup by rowgroup all loaded into memory
	 *
	 * @param processor the consumer of the rowgroups
	 */
	void processFile(ParquetColumnarProcessors.RowGroupProcessor processor);

	/**
	 * Process the entire parquet file, chunk by chunk
	 *
	 * @param processor the consumer of the chunks
	 */
	void processFile(ParquetColumnarProcessors.ChunkProcessor processor);

	/**
	 * Process the entire parquet file, chunk by chunk
	 *
	 * @param processor the consumer of the chunks
	 */
	void processFile(ParquetColumnarProcessors.ProcessRawChunkBytes processor);

	/**
	 * Get the footer metadata of the parquet file
	 *
	 * @return the footer metadata
	 */
	FileMetaData readMetaData() throws IOException;
}
