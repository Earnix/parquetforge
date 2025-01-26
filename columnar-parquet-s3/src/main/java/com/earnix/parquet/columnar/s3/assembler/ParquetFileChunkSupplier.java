package com.earnix.parquet.columnar.s3.assembler;

import com.earnix.parquet.columnar.file.reader.FileRangeInputStreamSupplier;
import com.earnix.parquet.columnar.file.reader.IndexedParquetColumnarFileReader;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.ColumnChunk;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

public class ParquetFileChunkSupplier implements ParquetColumnChunkSupplier
{
	private final Path parquetFilePath;
	private final ColumnDescriptor columnDescriptor;
	private final int rowGroup;

	// lazily computed
	private volatile boolean initialized;
	private ColumnChunk columnChunk;
	private FileRangeInputStreamSupplier inputStreamSupplier;

	public ParquetFileChunkSupplier(IndexedParquetColumnarFileReader reader, ColumnDescriptor columnDescriptor,
			int rowGroup)
	{
		this.parquetFilePath = reader.getParquetFilePath();
		this.columnDescriptor = columnDescriptor;
		this.rowGroup = rowGroup;
		initFromReader(reader);
	}

	/**
	 * Initialize a chunk bytes supplier from the specified file with the specified column descriptor. Implementation
	 * note - this MUST parse all footer metadata {@link org.apache.parquet.format.FileMetaData}. Prefer the constructor
	 * with the indexed reader if many chunks are used from the same parquet file.
	 *
	 * @param parquetFilePath  the path to the parquet file
	 * @param columnDescriptor the column descriptor of the column
	 * @param rowGroup         the row group offset
	 */
	public ParquetFileChunkSupplier(Path parquetFilePath, ColumnDescriptor columnDescriptor, int rowGroup)
	{
		this.parquetFilePath = parquetFilePath;
		this.columnDescriptor = columnDescriptor;
		this.rowGroup = rowGroup;
	}

	public ColumnDescriptor getColumnDescriptor()
	{
		return columnDescriptor;
	}

	private void ensureInitialized() throws IOException
	{
		if (!initialized)
		{
			synchronized (this)
			{
				if (!initialized)
				{
					IndexedParquetColumnarFileReader reader = new IndexedParquetColumnarFileReader(parquetFilePath);
					initFromReader(reader);
				}
			}
		}
	}

	private void initFromReader(IndexedParquetColumnarFileReader reader)
	{
		Pair<ColumnChunk, FileRangeInputStreamSupplier> chunk = reader.getInputStreamSupplier(rowGroup,
				columnDescriptor);
		this.columnChunk = chunk.getLeft();
		// data page offset is meaningless - MUST be set externally.
		this.columnChunk.getMeta_data().unsetData_page_offset();
		this.inputStreamSupplier = chunk.getRight();
		initialized = true;
	}

	@Override
	public long getNumRows()
	{
		return this.columnChunk.getMeta_data().getNum_values();
	}

	@Override
	public long getCompressedLength()
	{
		return this.columnChunk.getMeta_data().getTotal_compressed_size();
	}

	@Override
	public ColumnChunk getColumnChunk()
	{
		return this.columnChunk.deepCopy();
	}

	@Override
	public InputStream openInputStream() throws IOException
	{
		ensureInitialized();
		return inputStreamSupplier.get();
	}
}
