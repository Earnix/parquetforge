package com.earnix.parquet.columnar.file;

import com.earnix.parquet.columnar.utils.ColumnChunkForTesting;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

public class RowGroupForTesting
{
	private final long numRows;
	private final Set<ColumnChunkForTesting> columnChunks = new TreeSet<>(
			new TreeSet<>(Comparator.comparing(ColumnChunkForTesting::getColumnDescriptor)));

	public RowGroupForTesting(long numRows)
	{
		this.numRows = numRows;
	}

	public void addChunk(ColumnChunkForTesting columnChunkForTesting)
	{
		columnChunks.add(columnChunkForTesting);
	}

	public Set<ColumnChunkForTesting> getColumnChunks()
	{
		return columnChunks;
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
