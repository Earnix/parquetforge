package com.earnix.parquet.columnar.s3.assembler;

import org.apache.parquet.column.ColumnDescriptor;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ParquetRowGroupSupplier
{
	private final Map<ColumnDescriptor, ParquetColumnChunkSupplier> descriptorToSupplierMap;

	private ParquetRowGroupSupplier(Map<ColumnDescriptor, ParquetColumnChunkSupplier> descriptorToSupplierMap)
	{
		this.descriptorToSupplierMap = new HashMap<>(descriptorToSupplierMap);
	}

	public ParquetColumnChunkSupplier getSupplier(ColumnDescriptor columnDescriptor)
	{
		return Objects.requireNonNull(Objects.requireNonNull(descriptorToSupplierMap, "Chunk Descriptor cannot be null")
				.get(columnDescriptor), () -> "Chunk Supplier not found for " + columnDescriptor);
	}

	public static class Builder
	{
		private final ConcurrentMap<ColumnDescriptor, ParquetColumnChunkSupplier> descToSupplier = new ConcurrentHashMap<>();

		public ParquetRowGroupSupplier build()
		{
			return new ParquetRowGroupSupplier(descToSupplier);
		}

		public void addChunkSupplier(ColumnDescriptor descriptor, ParquetColumnChunkSupplier parquetColumnChunkSupplier)
		{
			Object old = this.descToSupplier.put(descriptor, parquetColumnChunkSupplier);
			if (old != null)
			{
				throw new IllegalStateException("Cannot add descriptor twice");
			}
		}
	}
}
