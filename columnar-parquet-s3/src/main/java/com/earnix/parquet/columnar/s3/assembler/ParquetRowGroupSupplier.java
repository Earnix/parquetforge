package com.earnix.parquet.columnar.s3.assembler;

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

	private ParquetRowGroupSupplier(Map<ColumnDescriptor, ParquetColumnChunkSupplier> descriptorToSupplierMap)
	{
		this.descriptorToSupplierMap = new HashMap<>(descriptorToSupplierMap);
	}

	/**
	 * Get the data supplier for a particular column
	 *
	 * @param columnDescriptor the descriptor of the column
	 * @return the parquet column chunk supplier
	 */
	public ParquetColumnChunkSupplier getSupplier(ColumnDescriptor columnDescriptor)
	{
		return Objects.requireNonNull(Objects.requireNonNull(descriptorToSupplierMap, "Chunk Descriptor cannot be null")
				.get(columnDescriptor), () -> "Chunk Supplier not found for " + columnDescriptor);
	}

	public static class Builder
	{
		private volatile long rowGroupSize = UNINITIALIZED_ROW_GROUP_SIZE;
		private final ConcurrentMap<ColumnDescriptor, ParquetColumnChunkSupplier> descToSupplier = new ConcurrentHashMap<>();

		public ParquetRowGroupSupplier build()
		{
			return new ParquetRowGroupSupplier(descToSupplier);
		}

		public void addChunkSupplier(ColumnDescriptor descriptor, ParquetColumnChunkSupplier parquetColumnChunkSupplier)
		{
			validateNumRows(descriptor, parquetColumnChunkSupplier);
			Object old = this.descToSupplier.put(descriptor, parquetColumnChunkSupplier);
			if (old != null)
			{
				throw new IllegalStateException("Cannot add descriptor twice");
			}
		}

		private void validateNumRows(ColumnDescriptor descriptor, ParquetColumnChunkSupplier parquetColumnChunkSupplier)
		{
			getOrInitNumRows(parquetColumnChunkSupplier);
			if (rowGroupSize != parquetColumnChunkSupplier.getNumRows())
			{
				throw new IllegalArgumentException(
						"Row group has " + rowGroupSize + " But Col Descriptor " + descriptor + " Row " + "Len: "
								+ parquetColumnChunkSupplier.getNumRows());
			}
		}

		private void getOrInitNumRows(ParquetColumnChunkSupplier parquetColumnChunkSupplier)
		{
			if (rowGroupSize == UNINITIALIZED_ROW_GROUP_SIZE)
			{
				synchronized (this)
				{
					if (rowGroupSize == UNINITIALIZED_ROW_GROUP_SIZE)
					{
						rowGroupSize = parquetColumnChunkSupplier.getNumRows();
					}
				}
			}
		}
	}
}
