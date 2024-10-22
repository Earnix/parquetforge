package com.earnix.parquet.columnar.utils;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;

public class ColumnChunkForTesting
{
	private final String primitiveTypeName;

	private final List<?> values;


	public ColumnChunkForTesting(String primitiveTypeName, List<?> values)
	{
		this.primitiveTypeName = primitiveTypeName;
		this.values = values;
	}

	public String getPrimitiveTypeName()
	{
		return primitiveTypeName;
	}

	@Override
	public int hashCode(){
		return new HashCodeBuilder()
				.append(primitiveTypeName)
				.append(values)
				.toHashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		ColumnChunkForTesting other = (ColumnChunkForTesting) obj;
		return new EqualsBuilder()
				.append(primitiveTypeName, other.primitiveTypeName)
				.append(values, other.values)
				.isEquals();
	}

	@Override
	public String toString(){
		return primitiveTypeName + ": " + values;
	}
}
