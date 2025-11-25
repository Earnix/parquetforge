package com.earnix.parquet.columnar.reader;

import com.earnix.parquet.columnar.reader.chunk.internal.ChunkDecompressToPageStoreFactory;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunkPageStore;
import com.earnix.parquet.columnar.utils.ParquetMagicUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.CompressionCodec;

import java.io.IOException;
import java.io.InputStream;

public class ParquetReaderUtils
{
	/**
	 * @param columnChunk the chunk to get the length of
	 * @return the length of the column chunk as stored in the parquet file
	 */
	public static long getLen(ColumnChunk columnChunk)
	{
		return columnChunk.getMeta_data().getTotal_compressed_size();
	}

	/**
	 * @param columnChunk the column chunk metadata
	 * @return the start offset of the column chunk in the parquet file.
	 */
	public static long getStartOffset(ColumnChunk columnChunk)
	{
		long startOffset = columnChunk.getMeta_data().getData_page_offset();

		// only use the dictionary as the start offset if it is valid. This should match the logic in the open source
		// java parquet driver in ParquetMetadataConverter.getOffset()
		if (columnChunk.getMeta_data().isSetDictionary_page_offset()
				&& columnChunk.getMeta_data().getDictionary_page_offset() > 0L
				&& columnChunk.getMeta_data().getDictionary_page_offset() < startOffset)
		{
			startOffset = columnChunk.getMeta_data().getDictionary_page_offset();
		}

		if (startOffset < ParquetMagicUtils.PARQUET_MAGIC.length())
			throw new IllegalArgumentException("Corrupted chunk metadata invalid startOffset.");

		return startOffset;
	}

	public static InMemChunk readInMemChunk(ColumnDescriptor colDescriptor, InputStream is, long chunkLen,
			CompressionCodec compressionCodec) throws IOException
	{
		// we do not close the input stream - this seems bad..
		InMemChunkPageStore inMemChunkPageStore = ChunkDecompressToPageStoreFactory.buildColumnChunkPageStore(
				colDescriptor, BoundedInputStream.builder().setInputStream(is).setMaxCount(chunkLen).get(), chunkLen,
				compressionCodec);
		return new InMemChunk(inMemChunkPageStore);
	}
}
