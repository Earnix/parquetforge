package com.earnix.parquet.columnar.writer.columnchunk;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;

import java.util.Iterator;
import java.util.PrimitiveIterator;

/**
 * An interface to write parquet files by column.
 */
public interface ColumnChunkWriter
{
	ColumnChunkPages writeColumn(ColumnDescriptor columnDescriptor, double[] vals);

	ColumnChunkPages writeColumn(ColumnDescriptor columnDescriptor, PrimitiveIterator.OfDouble iterator);

	ColumnChunkPages writeColumn(ColumnDescriptor columnDescriptor, NullableIterators.NullableDoubleIterator iterator);

	ColumnChunkPages writeColumn(ColumnDescriptor columnDescriptor, int[] vals);

	ColumnChunkPages writeColumn(ColumnDescriptor columnDescriptor, PrimitiveIterator.OfInt iterator);

	ColumnChunkPages writeColumn(ColumnDescriptor columnDescriptor, NullableIterators.NullableIntegerIterator iterator);

	ColumnChunkPages writeColumn(ColumnDescriptor columnDescriptor, long[] vals);

	ColumnChunkPages writeColumn(ColumnDescriptor columnDescriptor, PrimitiveIterator.OfLong iterator);

	ColumnChunkPages writeColumn(ColumnDescriptor columnDescriptor, NullableIterators.NullableLongIterator iterator);

	ColumnChunkPages writeColumn(ColumnDescriptor columnDescriptor, boolean[] vals);

	ColumnChunkPages writeColumn(ColumnDescriptor columnDescriptor, Iterator<Boolean> iterator);

	ColumnChunkPages writeColumn(ColumnDescriptor columnDescriptor, String[] vals);

	ColumnChunkPages writeStringColumn(ColumnDescriptor columnDescriptor, Iterator<String> vals);

	ColumnChunkPages writeBinaryColumnBytes(ColumnDescriptor columnDescriptor, Iterator<byte[]> vals);

	ColumnChunkPages writeBinaryColumn(ColumnDescriptor columnDescriptor, Iterator<Binary> vals);
}
