package com.earnix.parquet.columnar.s3.downloader;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesResponse;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectAttributes;
import software.amazon.awssdk.services.s3.model.ObjectPart;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Tool to handle downloading s3 objects in parts and mapping them to input streams with byte offsets
 *
 * <p>
 * Some caveats about s3 file parts<br>
 * </p>
 *
 * <p>
 * Max part number is 10,000.
 * </p>
 * <p>
 * Part numbers are not necessarily contiguous - s3 object may have parts 1,3,8, for example.
 * </p>
 * <p>
 * Parts must be at least 5MB, except for the last part of the object
 * </p>
 * <p>
 * List parts operation only returns up to 1000 parts. Need to make continuation request to get all parts
 * </p>
 */
public class S3KeyDownloader
{
	private static final int INVALID_OBJ_SIZE = -1;
	private final S3Client s3Client;
	private final String bucket;
	private final String key;
	private final Integer maxParts;
	private long objectSize = INVALID_OBJ_SIZE;
	private volatile List<ObjectPart> partsList = null;

	public S3KeyDownloader(S3Client s3Client, String bucket, String key)
	{
		this(s3Client, bucket, key, null);
	}

	public S3KeyDownloader(S3Client s3Client, String bucket, String key, Integer maxParts)
	{
		this.s3Client = s3Client;
		this.bucket = bucket;
		this.key = key;
		this.maxParts = maxParts;
	}

	public List<ObjectPart> getPartsList()
	{
		if (partsList == null)
		{
			synchronized (this)
			{
				if (partsList == null)
				{
					fetchPartsList();
				}
			}
		}
		return partsList;
	}

	private void fetchPartsList()
	{
		List<ObjectPart> fetchedPartsList = new ArrayList<>();

		GetObjectAttributesResponse resp = null;
		do
		{
			Integer partNumberMarker =
					resp == null || resp.objectParts() == null ? null : resp.objectParts().nextPartNumberMarker();
			resp = s3Client.getObjectAttributes(builder -> builder//
					.bucket(bucket)//
					.key(key)//
					.objectAttributes(ObjectAttributes.OBJECT_PARTS, ObjectAttributes.OBJECT_SIZE)//
					.partNumberMarker(partNumberMarker)//
					.maxParts(maxParts)//
			);

			if (resp.objectParts() != null)
				fetchedPartsList.addAll(resp.objectParts().parts());
		}
		while (resp.objectParts() != null && resp.objectParts().isTruncated());

		objectSize = resp.objectSize();

		// should be sorted, but double check..
		fetchedPartsList.sort(Comparator.comparingInt(ObjectPart::partNumber));

		partsList = Collections.unmodifiableList(fetchedPartsList);
	}

	public void downloadPart(int partNum, RespCallBack<ResponseInputStream<GetObjectResponse>> callback)
			throws IOException
	{
		try (ResponseInputStream<GetObjectResponse> response = s3Client.getObject(
				builder -> builder.bucket(bucket).key(key).partNumber(partNum)))
		{
			callback.callback(response);
		}
	}

	public void downloadRange(long startOffsetInclusive, long endOffsetExclusive,
			RespCallBack<ResponseInputStream<GetObjectResponse>> callback) throws IOException
	{
		try (ResponseInputStream<GetObjectResponse> response = openRange(startOffsetInclusive,
				endOffsetExclusive - startOffsetInclusive))
		{
			callback.callback(response);
		}
	}

	/**
	 * Open an input stream for a specific byte range. Caller is responsible for closing the returned stream.
	 *
	 * @param startOffsetInclusive the first byte in the range (inclusive)
	 * @param numBytesToRead       number of bytes to read starting at startOffsetInclusive
	 * @return an {@link ResponseInputStream} positioned at the requested range
	 */
	public ResponseInputStream<GetObjectResponse> openRange(long startOffsetInclusive, long numBytesToRead)
	{
		if (numBytesToRead <= 0)
		{
			throw new IllegalArgumentException("numBytesToRead must be > 0");
		}
		long endOffsetExclusive = startOffsetInclusive + numBytesToRead;
		String byteRange = getByteRangeHttpHeaderValue(startOffsetInclusive, endOffsetExclusive);
		return s3Client.getObject(builder -> builder.bucket(bucket).key(key).range(byteRange));
	}

	public long getObjectSize()
	{
		// ensure we initialized the field.
		getPartsList();
		return objectSize;
	}

	/**
	 * Read the trailing bytes of this file into memory
	 *
	 * @param startInclusive the start byte to read
	 * @param endExclusive   the end byte to read
	 * @return the specified bytes
	 */
	public byte[] getByteRange(long startInclusive, long endExclusive)
	{
		if (endExclusive <= startInclusive)
			throw new IllegalArgumentException("invalid range: " + startInclusive + " " + endExclusive);

		String byteRange = getByteRangeHttpHeaderValue(startInclusive, endExclusive);
		ResponseBytes<GetObjectResponse> resp = s3Client.getObjectAsBytes(
				builder -> builder.bucket(bucket).key(key).range(byteRange));
		return resp.asByteArrayUnsafe();
	}

	private static String getByteRangeHttpHeaderValue(long startInclusive, long endExclusive)
	{
		// note that http spec says the last byte is inclusive, so subtract one from end byte.
		String byteRange = "bytes=" + startInclusive + "-" + (endExclusive - 1);
		return byteRange;
	}

	/**
	 * Read the last n bytes of this s3 object into a byte array.
	 *
	 * @param numberOfBytes number of bytes to read
	 * @return the specified bytes
	 */
	public byte[] getLastBytes(int numberOfBytes)
	{
		String byteRange = "bytes=" + "-" + numberOfBytes;
		ResponseBytes<GetObjectResponse> resp = s3Client.getObjectAsBytes(
				builder -> builder.bucket(bucket).key(key).range(byteRange));
		return resp.asByteArrayUnsafe();
	}

	@FunctionalInterface
	public interface RespCallBack<T>
	{
		void callback(T resp) throws IOException;
	}
}
