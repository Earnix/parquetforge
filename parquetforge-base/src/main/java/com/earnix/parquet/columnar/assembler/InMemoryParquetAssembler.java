package com.earnix.parquet.columnar.assembler;

import com.earnix.parquet.columnar.utils.ParquetMagicUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.function.Uncheck;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Assemble a parquet file into an in memory byte array
 */
public class InMemoryParquetAssembler extends BaseParquetAssembler
{
	private final MessageType schema;

	public InMemoryParquetAssembler(MessageType schema)
	{
		this.schema = schema;
	}

	public byte[] assemble(List<ParquetRowGroupSupplier> rowGroups)
	{
		// first compute footer metadata
		List<ColumnDescriptor> columns = schema.getColumns();
		byte[] footerMetadata = buildSerializedMetadata(columns, rowGroups).toByteArray();

		int rowGroupsData = Math.toIntExact(
				rowGroups.stream().mapToLong(ParquetRowGroupSupplier::compressedBytes).sum());
		// compute size of destination byte array
		int parquetBytesLen =
				ParquetMagicUtils.PARQUET_MAGIC.length() + rowGroupsData + footerMetadata.length + Integer.BYTES
						+ ParquetMagicUtils.PARQUET_MAGIC.length();

		try (ByteArrayOutputStream parquetData = new ByteArrayOutputStream(parquetBytesLen))
		{
			// write magic
			IOUtils.copyLarge(ParquetMagicUtils.newMagicBytesInputStream(), parquetData);

			for (ParquetRowGroupSupplier parquetRowGroupSupplier : rowGroups)
			{
				for (ColumnDescriptor columnDescriptor : columns)
				{
					ParquetColumnChunkSupplier chunkSupplier = parquetRowGroupSupplier.getSupplier(columnDescriptor);
					try (InputStream inputStream = chunkSupplier.openInputStream())
					{
						IOUtils.copyLarge(inputStream, parquetData);
					}
				}
			}

			IOUtils.write(footerMetadata, parquetData);
			IOUtils.write(ParquetMagicUtils.createFooterAndMagic(footerMetadata.length), parquetData);

			byte[] ret = parquetData.toByteArray();
			if (ret.length != parquetBytesLen)
			{
				throw new IllegalStateException(
						"Unexpected Length. Expected: " + parquetBytesLen + " actual: " + ret.length);
			}
			return ret;
		}
		catch (IOException ex)
		{
			// should not happen
			throw new UncheckedIOException(ex);
		}
	}
}
