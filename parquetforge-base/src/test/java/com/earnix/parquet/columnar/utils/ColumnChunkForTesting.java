package com.earnix.parquet.columnar.utils;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.parquet.column.ColumnDescriptor;

import java.util.List;

public class ColumnChunkForTesting
{
	private final ColumnDescriptor columnDescriptor;

	private final List<?> values;


	public ColumnChunkForTesting(ColumnDescriptor columnDescriptor, List<?> values)
	{
		this.columnDescriptor = columnDescriptor;
		this.values = values;
	}

	public ColumnDescriptor getColumnDescriptor()
	{
		return columnDescriptor;
	}

	public long getValuesNumber()
	{
		return values.size();
	}

	@Override
	public int hashCode()
	{
		return new HashCodeBuilder().append(columnDescriptor).append(values).toHashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
		{
			return true;
		}
		if (obj == null || getClass() != obj.getClass())
		{
			return false;
		}
		ColumnChunkForTesting other = (ColumnChunkForTesting) obj;
		return new EqualsBuilder().append(columnDescriptor, other.columnDescriptor).append(values, other.values)
				.isEquals();
	}

	@Override
	public String toString()
	{
		return columnDescriptor + ": " + values;
	}
}
