package com.earnix.parquet.columnar.s3.assembler;

import com.earnix.parquet.columnar.s3.buffering.S3KeyUploader;
import com.earnix.parquet.columnar.s3.buffering.UploadPartUtils;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class S3ParquetAssembleAndUpload
{
	private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);

	private final MessageType schema;
	private final int targetNumParts;
	private final int uploadThreads;

	public S3ParquetAssembleAndUpload(MessageType schema, int targetNumParts, int uploadThreads)
	{
		this.schema = schema;
		this.targetNumParts = targetNumParts;
		this.uploadThreads = uploadThreads;
	}

	public void assembleAndUpload(S3KeyUploader uploader, List<ParquetRowGroupSupplier> rowGroups)
	{
		if (rowGroups == null)
			throw new NullPointerException("row groups list cannot be null.");
		if (rowGroups.isEmpty())
			throw new IllegalArgumentException("Row groups list cannot be empty");

		int numColumns = rowGroups.get(0).getNumColumns();

		List<ColumnDescriptor> orderedDescriptors = schema.getColumns();
		UnsynchronizedByteArrayOutputStream serializedMetadata = buildSerializedMetadata(orderedDescriptors, rowGroups);

		List<ParquetColumnChunkSupplier> orderedChunkSuppliers = orderChunkSuppliers(rowGroups, numColumns,
				orderedDescriptors);

		long[] relevantOffsets = computePartOffsets(rowGroups, numColumns, orderedChunkSuppliers);

		Iterator<ParquetColumnChunkSupplier> supplierIterator = orderedChunkSuppliers.iterator();

		long[] lens = new long[relevantOffsets.length - 1];
		List<List<Supplier<InputStream>>> grouped = groupColumnChunks(lens, relevantOffsets, supplierIterator,
				serializedMetadata.size(), () -> serializedMetadata.toInputStream());

		ExecutorService service = Executors.newFixedThreadPool(uploadThreads);
		try
		{
			for (List<Supplier<InputStream>> grp : grouped)
			{
				int partNum = uploader.getNextPartNum();

				Iterator<Supplier<InputStream>> it = grp.iterator();
				Supplier<InputStream> sequenceInputStream = () -> new SequenceInputStream(new Enumeration<InputStream>()
				{
					@Override
					public boolean hasMoreElements()
					{
						return it.hasNext();
					}

					@Override
					public InputStream nextElement()
					{
						return it.next().get();
					}
				});
				long len = -1;
				//TODO: compute length!!
				service.submit(() -> uploader.uploadPart(partNum, len, sequenceInputStream));
			}

			service.shutdown();
			try
			{
				service.awaitTermination(365, TimeUnit.DAYS);
			}
			catch (InterruptedException ex)
			{
				Thread.currentThread().interrupt();
				throw new IllegalStateException(ex);
			}
		}
		finally
		{
			service.shutdown();
		}
	}

	private static List<List<Supplier<InputStream>>> groupColumnChunks(long[] lens, long[] relevantOffsets,
			Iterator<ParquetColumnChunkSupplier> supplierIterator, int metadataSize,
			Supplier<InputStream> metadataIsSupplier)
	{
		List<List<Supplier<InputStream>>> grouped = new ArrayList<>(lens.length);

		long currPlace = 0;
		for (int i = 1; i < relevantOffsets.length; i++)
		{
			List<Supplier<InputStream>> currGroup = new ArrayList<>();

			if (grouped.isEmpty())
			{
				// we're first! add the magic.
				currGroup.add(() -> new ByteArrayInputStream(MAGIC));
				lens[i - 1] += MAGIC.length;
			}

			while (currPlace < relevantOffsets[i])
			{
				ParquetColumnChunkSupplier next = supplierIterator.next();
				currGroup.add(() -> {
					try
					{
						return next.openInputStream();
					}
					catch (IOException ex)
					{
						throw new UncheckedIOException(ex);
					}
				});
				lens[i - 1] += next.getCompressedLength();
				currPlace += next.getCompressedLength();
			}

			grouped.add(currGroup);
		}
		List<Supplier<InputStream>> lastGroup = grouped.get(grouped.size() - 1);

		// add metadata to the last group.
		//TODO: probably should put the metadata in the last part by itself for easier downloading of metadata by
		// itself..
		lastGroup.add(metadataIsSupplier);
		lens[lens.length - 1] += metadataSize;

		lastGroup.add(() -> new ByteArrayInputStream(MAGIC));
		lens[lens.length - 1] += MAGIC.length;

		return grouped;
	}

	private long[] computePartOffsets(List<ParquetRowGroupSupplier> rowGroups, int numColumns,
			List<ParquetColumnChunkSupplier> orderedChunkSuppliers)
	{
		int currOffsetIdx = 0;
		final long[] possibleOffsets = new long[rowGroups.size() * numColumns];
		long lastOffset = 0;
		for (ParquetColumnChunkSupplier columnChunkSupplier : orderedChunkSuppliers)
		{
			lastOffset = possibleOffsets[currOffsetIdx++] = lastOffset + columnChunkSupplier.getCompressedLength();
		}
		long[] relevantOffsets = UploadPartUtils.computePartDivisions(targetNumParts, possibleOffsets);
		if (relevantOffsets == null)
		{
			relevantOffsets = new long[] { 0, possibleOffsets[possibleOffsets.length - 1] };
		}
		return relevantOffsets;
	}

	private static List<ParquetColumnChunkSupplier> orderChunkSuppliers(List<ParquetRowGroupSupplier> rowGroups,
			int numColumns, List<ColumnDescriptor> orderedDescriptors)
	{
		List<ParquetColumnChunkSupplier> orderedChunkSuppliers = new ArrayList<>(rowGroups.size() * numColumns);

		for (ParquetRowGroupSupplier rowGroup : rowGroups)
		{
			for (ColumnDescriptor descriptor : orderedDescriptors)
			{
				ParquetColumnChunkSupplier columnChunkSupplier = rowGroup.getSupplier(descriptor);
				orderedChunkSuppliers.add(columnChunkSupplier);
			}
		}
		return orderedChunkSuppliers;
	}

	private UnsynchronizedByteArrayOutputStream buildSerializedMetadata(List<ColumnDescriptor> columnDescriptors,
			List<ParquetRowGroupSupplier> rowGroupSuppliers)
	{
		List<RowGroupInfo> rowGroupInfos = new ArrayList<>(rowGroupSuppliers.size());

		long rowGroupStartPlace = MAGIC.length;
		for (ParquetRowGroupSupplier prgs : rowGroupSuppliers)
		{
			long chunkStartPlaceInFile = rowGroupStartPlace;
			List<ColumnChunkInfo> chunkInfoList = new ArrayList<>(columnDescriptors.size());
			for (ColumnDescriptor columnDescriptor : columnDescriptors)
			{
				ParquetColumnChunkSupplier parquetColumnChunkSupplier = prgs.getSupplier(columnDescriptor);
				ColumnChunkInfo chunkInfo = new FullColumnChunkInfo(columnDescriptor,
						parquetColumnChunkSupplier.getColumnChunk(), rowGroupStartPlace);
				chunkInfoList.add(chunkInfo);
				rowGroupStartPlace += parquetColumnChunkSupplier.getCompressedLength();
			}

			RowGroupInfo rowGroupInfo = new RowGroupInfo(rowGroupStartPlace, chunkStartPlaceInFile - rowGroupStartPlace,
					chunkInfoList);
			rowGroupInfos.add(rowGroupInfo);

			rowGroupStartPlace = chunkStartPlaceInFile;
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
