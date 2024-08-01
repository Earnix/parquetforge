package com.earnix.parquet.columnar.reader.chunk;

import java.io.IOException;
import java.io.InputStream;

import com.earnix.parquet.columnar.reader.chunk.internal.ChunkDecompressToPageStoreFactory;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.CompressionCodec;

/**
 * A utils class for
 */
public class ChunkReader
{
	/**
	 * Read all the pages of the column chunk into memory with a deserialized dictionary
	 * 
	 * @param descriptor
	 * @param is
	 * @param byteLimit
	 * @param codec
	 * @return
	 * @throws IOException
	 */
	public static InMemChunk readChunk(ColumnDescriptor descriptor, InputStream is, long byteLimit,
									   CompressionCodec codec) throws IOException
	{
		return new InMemChunk(ChunkDecompressToPageStoreFactory.buildPageStore(descriptor, new CountingInputStream(is),
				byteLimit, codec));
	}
}
