package com.earnix.parquet.columnar.s3.assembler;

import com.earnix.parquet.columnar.reader.FileRangeInputStreamSupplier;
import com.earnix.parquet.columnar.reader.IndexedParquetColumnarFileReader;
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


	public ParquetFileChunkSupplier(Path parquetFilePath, ColumnDescriptor columnDescriptor, int rowGroup)
	{
		this.parquetFilePath = parquetFilePath;
		this.columnDescriptor = columnDescriptor;
		this.rowGroup = rowGroup;
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
					Pair<ColumnChunk, FileRangeInputStreamSupplier> chunk = reader.getInputStreamSupplier(rowGroup,
							columnDescriptor);
					this.columnChunk = chunk.getLeft();
					// data page offset is meaningless - MUST be set externally.
					this.columnChunk.getMeta_data().unsetData_page_offset();
					this.inputStreamSupplier = chunk.getRight();
					initialized = true;
				}
			}
		}
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
