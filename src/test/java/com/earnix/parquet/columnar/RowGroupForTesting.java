package com.earnix.parquet.columnar;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.List;

public class RowGroupForTesting
{
	private final long numRows;
	private final List<ColumnChunkForTesting> columnChunks = new ArrayList<>();

	public RowGroupForTesting(long numRows)
	{
		this.numRows = numRows;
	}

	public void addChunk(ColumnChunkForTesting columnChunkForTesting)
	{
		columnChunks.add(columnChunkForTesting);
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
		RowGroupForTesting other = (RowGroupForTesting) obj;
		return new EqualsBuilder()
				.append(numRows, other.numRows)
				.append(columnChunks, other.columnChunks)
				.isEquals();
	}

	@Override
	public int hashCode(){
		return new HashCodeBuilder()
				.append(numRows)
				.append(columnChunks)
				.toHashCode();
	}

	@Override
	public String toString(){
		return numRows + ", " + columnChunks;
	}
}
