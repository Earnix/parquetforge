package com.earnix.parquet.columnar.assembler;

import org.apache.parquet.column.ColumnDescriptor;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A supplier that can produce input streams for all chunks within a row group.
 */
public class ParquetRowGroupSupplier
{
	private static final int UNINITIALIZED_ROW_GROUP_SIZE = -1;
	private final Map<ColumnDescriptor, ParquetColumnChunkSupplier> descriptorToSupplierMap;
	private final long numRows;

	/**
	 * @return a builder to make a parquet row group
	 */
	public static Builder builder()
	{
		return new Builder();
	}

	private ParquetRowGroupSupplier(Map<ColumnDescriptor, ParquetColumnChunkSupplier> descriptorToSupplierMap,
			long numRows)
	{
		this.descriptorToSupplierMap = new HashMap<>(descriptorToSupplierMap);
		this.numRows = numRows;
	}

	/**
	 * Get the data supplier for a particular column
	 *
	 * @param columnDescriptor the descriptor of the column
	 * @return the parquet column chunk supplier
	 */
	public ParquetColumnChunkSupplier getSupplier(ColumnDescriptor columnDescriptor)
	{
		ParquetColumnChunkSupplier chunkSupplier = descriptorToSupplierMap.get(
				Objects.requireNonNull(columnDescriptor, "Chunk Descriptor cannot be null"));
		return Objects.requireNonNull(chunkSupplier, () -> "Chunk Supplier not found for " + columnDescriptor);
	}

	/**
	 * Get the number of columns in this Row Group
	 *
	 * @return the number of columns in this row group
	 */
	public int getNumColumns()
	{
		return this.descriptorToSupplierMap.size();
	}

	/**
	 * @return the number of rows in this row group
	 */
	public long getNumRows()
	{
		return numRows;
	}

	/**
	 * @return the total number of bytes used by this row group
	 */
	public long compressedBytes()
	{
		return descriptorToSupplierMap.values().stream()// all of the column chunks
				.mapToLong(ParquetColumnChunkSupplier::getCompressedLength).sum();
	}

	public static class Builder
	{
		private volatile long numRows = UNINITIALIZED_ROW_GROUP_SIZE;
		private final ConcurrentMap<ColumnDescriptor, ParquetColumnChunkSupplier> descToSupplier = new ConcurrentHashMap<>();

		private Builder()
		{
		}

		public ParquetRowGroupSupplier build()
		{
			return new ParquetRowGroupSupplier(descToSupplier, numRows);
		}

		/**
		 * Add a supplier for a column
		 *
		 * @param parquetColumnChunkSupplier the supplier for the specified column
		 * @return the supplier for the specified column
		 */
		public Builder addChunkSupplier(ParquetColumnChunkSupplier parquetColumnChunkSupplier)
		{
			ColumnDescriptor descriptor = parquetColumnChunkSupplier.getColumnDescriptor();
			validateNumRows(descriptor, parquetColumnChunkSupplier);
			Object old = this.descToSupplier.put(descriptor, parquetColumnChunkSupplier);
			if (old != null)
			{
				throw new IllegalStateException("Cannot add descriptor twice");
			}
			return this;
		}

		private void validateNumRows(ColumnDescriptor descriptor, ParquetColumnChunkSupplier parquetColumnChunkSupplier)
		{
			getOrInitNumRows(parquetColumnChunkSupplier);
			if (numRows != parquetColumnChunkSupplier.getNumRows())
			{
				throw new IllegalArgumentException(
						"Row group has " + numRows + " But Col Descriptor " + descriptor + " Row " + "Len: "
								+ parquetColumnChunkSupplier.getNumRows());
			}
		}

		private void getOrInitNumRows(ParquetColumnChunkSupplier parquetColumnChunkSupplier)
		{
			if (numRows == UNINITIALIZED_ROW_GROUP_SIZE)
			{
				synchronized (this)
				{
					if (numRows == UNINITIALIZED_ROW_GROUP_SIZE)
					{
						numRows = parquetColumnChunkSupplier.getNumRows();
					}
				}
			}
		}
	}
}
