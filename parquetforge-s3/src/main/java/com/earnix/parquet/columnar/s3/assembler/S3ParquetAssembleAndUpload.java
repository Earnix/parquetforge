package com.earnix.parquet.columnar.s3.assembler;

import com.earnix.parquet.columnar.assembler.BaseParquetAssembler;
import com.earnix.parquet.columnar.assembler.ParquetColumnChunkSupplier;
import com.earnix.parquet.columnar.assembler.ParquetRowGroupSupplier;
import com.earnix.parquet.columnar.reader.ParquetMetadataUtils;
import com.earnix.parquet.columnar.s3.buffering.S3KeyUploader;
import com.earnix.parquet.columnar.s3.buffering.UploadPartUtils;
import com.earnix.parquet.columnar.utils.ParquetMagicUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.KeyValue;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * An interface to assemble a parquet file on S3 from source parquet files with zero copying
 */
public class S3ParquetAssembleAndUpload extends BaseParquetAssembler
{
	private static final Logger LOG = LoggerFactory.getLogger(S3ParquetAssembleAndUpload.class);
	private static final AtomicLong threadPoolIDNumber = new AtomicLong();

	private final MessageType schema;
	private final List<KeyValue> keyValueMetadata;
	private final int targetNumParts;
	private final int uploadThreads;

	/**
	 * @param schema         the schema of the assembled parquet file
	 * @param targetNumParts the target number of parts to use when uploading the file in parts onto S3
	 * @param uploadThreads  the number of threads to use when uploading to s3
	 */
	public S3ParquetAssembleAndUpload(MessageType schema, List<KeyValue> keyValuesMetadata, int targetNumParts,
			int uploadThreads)
	{
		this.schema = schema;
		this.keyValueMetadata = ParquetMetadataUtils.deepCopyKeyValueMetadata(keyValuesMetadata);
		this.targetNumParts = targetNumParts;
		this.uploadThreads = uploadThreads;
	}

	/**
	 * Assemble and upload a parquet file on s3
	 *
	 * @param uploader  the uploader for the key on s3
	 * @param rowGroups the suppliers of data for row groups
	 */
	public void assembleAndUpload(S3KeyUploader uploader, List<ParquetRowGroupSupplier> rowGroups)
	{
		if (rowGroups == null)
			throw new NullPointerException("row groups list cannot be null.");
		if (rowGroups.isEmpty())
			throw new IllegalArgumentException("Row groups list cannot be empty");

		int numColumns = rowGroups.get(0).getNumColumns();

		List<ColumnDescriptor> orderedDescriptors = schema.getColumns();
		UnsynchronizedByteArrayOutputStream serializedMetadata = BaseParquetAssembler.buildSerializedMetadata(
				orderedDescriptors, rowGroups, keyValueMetadata);

		List<ParquetColumnChunkSupplier> orderedChunkSuppliers = orderChunkSuppliers(rowGroups, numColumns,
				orderedDescriptors);

		long[] relevantOffsets = computePartOffsets(rowGroups, numColumns, orderedChunkSuppliers);

		Iterator<ParquetColumnChunkSupplier> supplierIterator = orderedChunkSuppliers.iterator();

		long[] lens = new long[relevantOffsets.length - 1];
		List<List<Supplier<InputStream>>> grouped = groupColumnChunks(lens, relevantOffsets, supplierIterator,
				serializedMetadata.size(), serializedMetadata::toInputStream);

		// this can be moved to automatically managed resource once we upgrade to a newer java version.
		ExecutorService service = new ThreadPoolExecutor(uploadThreads, uploadThreads, 0L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<>(), new ThreadFactoryBuilder().setNameFormat(
				"parquet-assembler-s3-uploader-" + threadPoolIDNumber.incrementAndGet() + "-%d").build());
		boolean success = false;
		try
		{
			int lenIdx = 0;
			for (List<Supplier<InputStream>> grp : grouped)
			{
				int partNum = uploader.getNextPartNum();
				Supplier<InputStream> sequenceInputStream = createInputStreamSupplier(grp);
				long len = lens[lenIdx++];
				service.submit(() -> uploader.uploadPart(partNum, len, sequenceInputStream));
			}

			//TODO: need to validate all upload jobs completed correctly.
			service.shutdown();
			try
			{
				boolean uploadsCompleted = service.awaitTermination(365, TimeUnit.DAYS);
				if (!uploadsCompleted)
					throw new IllegalStateException("uploads did not complete within timeout");
			}
			catch (InterruptedException ex)
			{
				Thread.currentThread().interrupt();
				throw new IllegalStateException(ex);
			}
			uploader.finish();
			success = true;
		}
		finally
		{
			service.shutdown();
			try
			{
				if (!service.awaitTermination(365, TimeUnit.DAYS))
				{
					LOG.warn("S3 Parquet Assembler Uploader executor service did not shutdown {}", service);
				}
			}
			catch (InterruptedException exception)
			{
				LOG.warn("S3 Parquet Assembler Uploader executor service did not shutdown due to interrupt {}",
						service);
				Thread.currentThread().interrupt();
			}

			if (!success)
			{
				try
				{
					uploader.abortUpload();
				}
				catch (Exception ex)
				{
					// exception in finally block, ignore.
				}
			}
		}
	}

	private static Supplier<InputStream> createInputStreamSupplier(List<Supplier<InputStream>> grp)
	{
		// Beware: This code is fragile. We intentionally do NOT use Collections.enumeration.
		// This code ensures that an InputStream is created ONLY when it is ready to be read, and not before.
		// This is important because we may have many InputStreams in many different files, and we don't want to run
		// out of file descriptors - only one file should be open at a time.
		return () -> {
			Iterator<Supplier<InputStream>> it = grp.iterator();
			return new SequenceInputStream(new Enumeration<>()
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
		};
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
				currGroup.add(ParquetMagicUtils::newMagicBytesInputStream);
				lens[i - 1] += ParquetMagicUtils.PARQUET_MAGIC.length();
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

		byte[] footerLenAndMagic = ParquetMagicUtils.createFooterAndMagic(metadataSize);
		lastGroup.add(() -> new ByteArrayInputStream(footerLenAndMagic));
		lens[lens.length - 1] += footerLenAndMagic.length;

		return grouped;
	}

	private long[] computePartOffsets(List<ParquetRowGroupSupplier> rowGroups, int numColumns,
			List<ParquetColumnChunkSupplier> orderedChunkSuppliers)
	{
		int currOffsetIdx = 0;
		final long[] possibleOffsets = new long[1 + rowGroups.size() * numColumns];
		long lastOffset = 0;
		for (ParquetColumnChunkSupplier columnChunkSupplier : orderedChunkSuppliers)
		{
			lastOffset = possibleOffsets[++currOffsetIdx] = lastOffset + columnChunkSupplier.getCompressedLength();
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
}
