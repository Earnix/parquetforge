package com.earnix.parquet.columnar.reader;

import com.earnix.parquet.columnar.reader.chunk.internal.InMemChunk;
import org.apache.commons.io.function.IOSupplier;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;

import java.io.IOException;
import java.io.InputStream;

/**
 * A parquet columnar reader that indexes columns and row groups to allow reading arbitrary columns and row groups by
 * raw input streams, and into {@link InMemChunk}. Also supports fetching ColumnChunk metadata data from for arbitrary
 * columns without reading the column pages.
 * <p> All lookups are done by maps that are constructed when this reader is built to ensure O(1) performance
 * regardless of how many columns and row groups are present.</p>
 * <p>Note that this does NOT need to be closed. This is because the offset indices are created on construction,
 * and the file is opened and closed everytime when individual chunks are read</p>
 */
public interface IndexedParquetColumnarReader extends BaseColumnarReader
{
	/**
	 * Read a specific column chunk in a row group into memory
	 *
	 * @param rowGroup   the row group offset
	 * @param descriptor the column to read into memory
	 * @return the in memory chunk.
	 * @throws IOException on failure to read in the file
	 */
	InMemChunk readInMem(int rowGroup, ColumnDescriptor descriptor) throws IOException;

	/**
	 * Fetch the footer metadata for the specified column. Note: This returns a COPY of the metadata to prevent the
	 * ColumnChunk thrift object from being inadvertently modified. Avoid calling this within a loop.
	 *
	 * @param rowGroup   the row group number
	 * @param descriptor the column descriptor
	 * @return the ColumnChunk metadata present within the footer metadata
	 */
	ColumnChunk getColumnChunk(int rowGroup, ColumnDescriptor descriptor);

	/**
	 * Get an input stream supplier for a specific column chunk. This function returns {@link ColumnChunk} too because
	 * it is already looked up, and it is necessary to read the column because of the need to know what
	 * {@link org.apache.parquet.format.CompressionCodec} is used. Note that this is a deep copy of {@link ColumnChunk}
	 * and as such this should not be called in a loop<br>
	 * <p>
	 * An input stream supplier is returned rather than an input stream because there are use cases for needing to
	 * construct multiple input streams for the same column chunk, such as when uploading to an object storage and
	 * needing to retry due to a network interruption.
	 * </p>
	 *
	 * @param rowGroup   the row group offset
	 * @param descriptor the column descriptor
	 * @return the column chunk and input stream supplier
	 */
	Pair<ColumnChunk, IOSupplier<InputStream>> getInputStreamSupplier(int rowGroup, ColumnDescriptor descriptor);

	/**
	 * Get a column descriptor by its path
	 *
	 * @param path the path of the column descriptor
	 * @return the column descriptor
	 */
	ColumnDescriptor getDescriptorByPath(String... path);
}
