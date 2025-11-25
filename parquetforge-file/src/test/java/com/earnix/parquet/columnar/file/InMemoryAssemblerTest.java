package com.earnix.parquet.columnar.file;

import com.earnix.parquet.columnar.assembler.InMemoryParquetAssembler;
import com.earnix.parquet.columnar.assembler.ParquetRowGroupSupplier;
import com.earnix.parquet.columnar.file.assembler.ParquetFileChunkSupplier;
import com.earnix.parquet.columnar.file.reader.ParquetFileReaderFactory;
import com.earnix.parquet.columnar.reader.IndexedParquetColumnarReader;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public class InMemoryAssemblerTest
{
	/**
	 * A very simple use case of the in memory assembler
	 */
	@Test
	public void basicTest() throws Exception
	{
		Path tmp = Files.createTempFile("parquetFile", ".parquet");
		Path tmp2 = Files.createTempFile("parquetFile", ".parquet");
		try
		{
			// write a basic single double column parquet file
			ParquetFileFiller.basicSingleDoubleColumnParquetFile(tmp);

			IndexedParquetColumnarReader reader = ParquetFileReaderFactory.createIndexedColumnarFileReader(tmp);
			InMemoryParquetAssembler assembler = new InMemoryParquetAssembler(
					new MessageType("root", reader.getDescriptor(0).getPrimitiveType()));
			byte[] parquetBytes = assembler.assemble(Arrays.asList(ParquetRowGroupSupplier.builder()
					.addChunkSupplier(new ParquetFileChunkSupplier(reader, reader.getDescriptor(0), 0)).build()));

			Files.write(tmp2, parquetBytes, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
			IndexedParquetColumnarReader reader2 = ParquetFileReaderFactory.createIndexedColumnarFileReader(tmp2);


			ColEqualityCheckerUtils.assertDoubleColumnEqual(reader.readInMem(0, reader.getDescriptor(0)),
					reader2.readInMem(0, reader2.getDescriptor(0)));
		}
		finally
		{
			Files.deleteIfExists(tmp);
		}
	}
}
