package com.earnix.parquet.columnar.assembler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A list of row groups used to assemble a parquet file. Runs sanity checks to ensure that row groups are compatible
 * which each other
 */
public class ParquetRowGroups
{
	private final List<ParquetRowGroupSupplier> rowGroupSupplierList;

	public ParquetRowGroups(List<ParquetRowGroupSupplier> rowGroupSupplierList)
	{
		this.rowGroupSupplierList = Collections.unmodifiableList(new ArrayList<>(rowGroupSupplierList));
		sanityCheck();
	}

	private void sanityCheck()
	{

	}

	public List<ParquetRowGroupSupplier> getRowGroupSupplierList()
	{
		return rowGroupSupplierList;
	}
}
