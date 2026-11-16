package com.earnix.parquet.columnar;

import com.earnix.parquet.columnar.assembler.ChunkPagesParquetColumnChunkSupplier;
import com.earnix.parquet.columnar.assembler.InMemoryParquetAssembler;
import com.earnix.parquet.columnar.assembler.ParquetRowGroupSupplier;
import com.earnix.parquet.columnar.reader.IndexedParquetColumnarReader;
import com.earnix.parquet.columnar.utils.ParquetInMemReaderUtils;
import com.earnix.parquet.columnar.writer.columnchunk.ColumnChunkPages;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static com.earnix.parquet.columnar.utils.EmptyParquetInMemWriter.inMemWriteEmptyParquetFile;

public class InMemReadWriteTest
{
	@Test
	public void testEmptyFileRead() throws Exception
	{
		MessageType schema = new MessageType("root",
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, "col1"));
		byte[] emptyParquetFile = inMemWriteEmptyParquetFile(schema, List.of());

		Optional<IndexedParquetColumnarReader> reader = ParquetInMemReaderUtils.readIntoIndexParquetColumnarReader(
				emptyParquetFile);
		Assert.assertTrue(reader.isPresent());

		Assert.assertEquals(0, reader.get().getTotalNumRows());
		Assert.assertEquals(0, reader.get().getNumRowGroups());
	}

	@Test
	public void testSimpleReadWrite() throws Exception
	{
		double[] vals = { 1, 2, 3 };
		MessageType schema = new MessageType("root",
				new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, "col1"));

		InMemoryParquetAssembler assembler = new InMemoryParquetAssembler(schema, List.of());
		ColumnDescriptor descriptor = schema.getColumns().get(0);
		ColumnChunkPages pages = ColumnChunkWriterReaderTest.doublePage(vals, CompressionCodec.UNCOMPRESSED,
				descriptor);

		byte[] bytes = assembler.assemble(List.of(ParquetRowGroupSupplier.builder()
				.addChunkSupplier(new ChunkPagesParquetColumnChunkSupplier(pages)).build()));

		Optional<IndexedParquetColumnarReader> reader = ParquetInMemReaderUtils.readIntoIndexParquetColumnarReader(
				bytes);
		Assert.assertTrue(reader.isPresent());

		var chunk = reader.get().readInMem(0, descriptor);
		Assert.assertEquals(3, chunk.getTotalValues());
		Assert.assertEquals(3, reader.get().getTotalNumRows());
	}
}
