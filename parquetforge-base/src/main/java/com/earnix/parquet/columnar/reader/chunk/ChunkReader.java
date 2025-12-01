package com.earnix.parquet.columnar.reader.chunk;

import com.earnix.parquet.columnar.reader.chunk.internal.ChunkDecompressToPageStoreFactory;
import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.CompressionCodec;

import java.io.IOException;
import java.io.InputStream;

/**
 * A utils class for constructing an {@link InMemChunk} from an arbitrary input stream
 */
public class ChunkReader
{
	/**
	 * Read all the pages of the column chunk into memory with a deserialized dictionary
	 *
	 * @param descriptor column descriptor of the chunk
	 * @param is         the input stream
	 * @param byteLimit  the number of bytes to read from this stream
	 * @param codec      the compression codec to decompress
	 * @return the decoded chunk
	 * @throws IOException error reading from input stream or possibly decompression
	 */
	public static InMemChunk readChunk(ColumnDescriptor descriptor, InputStream is, long byteLimit,
			CompressionCodec codec) throws IOException
	{
		return new InMemChunk(ChunkDecompressToPageStoreFactory.buildColumnChunkPageStore(descriptor,
				BoundedInputStream.builder().setInputStream(is).setMaxCount(byteLimit).get(), byteLimit, codec));
	}
}
