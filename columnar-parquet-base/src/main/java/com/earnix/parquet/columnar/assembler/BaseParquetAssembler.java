package com.earnix.parquet.columnar.assembler;

import com.earnix.parquet.columnar.utils.ParquetMagicUtils;
import com.earnix.parquet.columnar.writer.ParquetWriterUtils;
import com.earnix.parquet.columnar.writer.rowgroup.ColumnChunkInfo;
import com.earnix.parquet.columnar.writer.rowgroup.FullColumnChunkInfo;
import com.earnix.parquet.columnar.writer.rowgroup.RowGroupInfo;
import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Util;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BaseParquetAssembler
{
	public static UnsynchronizedByteArrayOutputStream buildSerializedMetadata(List<ColumnDescriptor> columnDescriptors,
			List<ParquetRowGroupSupplier> rowGroupSuppliers)
	{
		List<RowGroupInfo> rowGroupInfos = new ArrayList<>(rowGroupSuppliers.size());

		long currOffsetInFile = ParquetMagicUtils.PARQUET_MAGIC.length();
		for (ParquetRowGroupSupplier prgs : rowGroupSuppliers)
		{
			// Why is this unused?!?!
			long rowGroupStartOffset = currOffsetInFile;
			List<ColumnChunkInfo> chunkInfoList = new ArrayList<>(columnDescriptors.size());
			for (ColumnDescriptor columnDescriptor : columnDescriptors)
			{
				ParquetColumnChunkSupplier parquetColumnChunkSupplier = prgs.getSupplier(columnDescriptor);
				ColumnChunkInfo chunkInfo = new FullColumnChunkInfo(columnDescriptor,
						parquetColumnChunkSupplier.getColumnChunk(), currOffsetInFile);
				chunkInfoList.add(chunkInfo);
				currOffsetInFile += parquetColumnChunkSupplier.getCompressedLength();
			}

			RowGroupInfo rowGroupInfo = new RowGroupInfo(currOffsetInFile, prgs.getNumRows(), chunkInfoList);
			rowGroupInfos.add(rowGroupInfo);
		}

		List<SchemaElement> schemaElements = ParquetWriterUtils.getSchemaElements(new MessageType("root",
				columnDescriptors.stream().map(ColumnDescriptor::getPrimitiveType).collect(Collectors.toList())));
		FileMetaData parquetFooterMetadata = ParquetWriterUtils.getFileMetaData(rowGroupInfos, schemaElements);
		UnsynchronizedByteArrayOutputStream byteArrayOutputStream = UnsynchronizedByteArrayOutputStream.builder().get();

		try
		{
			Util.writeFileMetaData(parquetFooterMetadata, byteArrayOutputStream);
			return byteArrayOutputStream;
		}
		catch (IOException ex)
		{
			throw new UncheckedIOException(ex);
		}
	}
}
