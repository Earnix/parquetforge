package com.earnix.parquet.columnar.s3.buffering;

import com.earnix.parquet.columnar.s3.S3Constants;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shaded.parquet.it.unimi.dsi.fastutil.objects.ObjectIntImmutablePair;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * An abstraction for uploading a parquet Object to S3 in multiple parts.
 */
public class S3KeyUploader implements AutoCloseable
{
	private static final Logger LOG = LoggerFactory.getLogger(S3KeyUploader.class);

	private static final boolean ADD_ASSERTIONS = false;
	// TODO: this should be made configurable
	private static final int MAX_REQ_PER_SECOND = 500;
	private final RateLimiter rateLimiter = RateLimiter.create(MAX_REQ_PER_SECOND);

	private final S3Client s3Client;
	private final String bucket;
	private final String key;
	private final String mimeType;
	private final List<ObjectIntImmutablePair<UploadPartResponse>> uploadPartResponseList = Collections.synchronizedList(
			new ArrayList<>());

	// lazily populated.
	private volatile String uploadId = null;
	private boolean isCancelled;
	private boolean isDone;

	private int partNum = 1;

	/**
	 * Create a new S3 key uploader
	 *
	 * @param s3Client the synchronous client for uploading s3 parts
	 * @param bucket   the bucket to upload the parts to
	 * @param key      the key to upload
	 */
	public S3KeyUploader(S3Client s3Client, String bucket, String key)
	{
		this(s3Client, bucket, key, "application/octet-stream");
	}

	private S3KeyUploader(S3Client s3Client, String bucket, String key, String mimeType)
	{
		this.s3Client = s3Client;
		this.bucket = bucket;
		this.key = key;
		this.mimeType = mimeType;
	}

	/**
	 * Get the next part number to upload for this key. Note that this function is NOT threadsafe, as S3 file part order
	 * matters. So multithreaded calls make no semantic sense. This function is separated from
	 * {@link #uploadPart(int, long, Supplier)} so that multiple parts can be uploaded to S3 in parallel
	 *
	 * @return the next part number
	 */
	public int getNextPartNum()
	{
		if (partNum > S3Constants.MAX_PARTS_PER_FILE)
			throw new IllegalStateException("too many parts for s3");

		return partNum++;
	}

	/**
	 * Upload the part to S3. This function may be called in multiple threads
	 *
	 * @param partNum the part number to upload
	 * @param len     the length of the data being uploaded for this part
	 * @param is      the InputStream supplier for the data. Note that this may be called multiple times if the upload
	 *                call to S3 fails and is retried
	 */
	public void uploadPart(int partNum, long len, Supplier<InputStream> is)
	{
		// prefer using the supplier lazily so that if the upload call fails and needs to be retried, a new input
		// stream will be created.
		RequestBody requestBody = RequestBody.fromContentProvider(new ContentStreamProvider()
		{
			private InputStream openIs = null;

			@Override
			public InputStream newStream()
			{
				if (openIs != null)
				{
					try
					{
						openIs.close();
					}
					catch (IOException ex)
					{
						// ignore
					}
				}
				openIs = is.get();
				return openIs;
			}
		}, len, mimeType);

		rateLimiter.acquire();

		String createdUploadId = getOrCreateUploadId();
		if (ADD_ASSERTIONS)
		{
			LOG.info("Upload Part key: {} uploadId: {} partNumber: {} len: {}", key, createdUploadId, partNum, len);
			try
			{
				int computedLen = IOUtils.toByteArray(is.get()).length;
				if (computedLen != len)
					throw new IllegalArgumentException("len is wrong");
			}
			catch (IOException ex)
			{
				throw new UncheckedIOException(ex);
			}
		}

		UploadPartResponse resp = s3Client.uploadPart(builder -> customizeUploadPartRequest(
				builder.bucket(bucket).key(key).uploadId(createdUploadId).partNumber(partNum)), requestBody);
		LOG.debug("Finished upload for {} uploadId: {} partNum: {} resp: {}", getS3UploadUri(), uploadId, partNum,
				resp);

		uploadPartResponseList.add(new ObjectIntImmutablePair<>(resp, partNum));
	}

	/**
	 * Completes the upload to S3 via {@link S3Client#completeMultipartUpload(CompleteMultipartUploadRequest)} Note: all
	 * calls to {@link #uploadPart(int, long, Supplier)} MUST be completed BEFORE this function is called.
	 */
	public synchronized void finish()
	{
		if (this.isDone)
			throw new IllegalStateException("Already Done");
		if (this.isCancelled)
			throw new IllegalStateException("Already Cancelled.");

		this.isDone = true;

		CompletedPart[] completedParts = this.uploadPartResponseList.stream()//
				.sorted(Comparator.comparingInt(ObjectIntImmutablePair::rightInt))//sort by part number
				.map(resp -> CompletedPart.builder().partNumber(resp.rightInt()).eTag(resp.left().eTag()).build())
				.toArray(CompletedPart[]::new);

		LOG.debug("Found {} completed parts for {} uploadId: {}", completedParts.length, getS3UploadUri(), uploadId);
		for (CompletedPart completedPart : completedParts)
		{
			LOG.trace("S3Object: {} CompletedPart {}", getS3UploadUri(), completedPart);
		}

		CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder().parts(completedParts)
				.build();

		rateLimiter.acquire();
		s3Client.completeMultipartUpload(builder -> customizeCompleteMultipartUploadRequest(
				builder.bucket(bucket).key(key).uploadId(Objects.requireNonNull(uploadId))
						.multipartUpload(completedMultipartUpload)));
	}

	/**
	 * Cancels the upload and deletes any partial uploads to this part
	 */
	public synchronized void abortUpload()
	{
		isCancelled = true;
		if (uploadId != null)
		{
			rateLimiter.acquire();
			s3Client.abortMultipartUpload(builder -> customizeAbortMultipartUploadRequest(
					builder.bucket(bucket).key(key).uploadId(uploadId)));
			uploadId = null;
		}
	}

	/**
	 * Gets the Upload ID for this multi part upload. Lazily created
	 *
	 * @return the Upload ID
	 */
	private String getOrCreateUploadId()
	{
		if (uploadId == null)
		{
			synchronized (this)
			{
				if (uploadId == null)
				{
					if (isCancelled)
						throw new IllegalStateException("Upload was cancelled");

					// start new multipart upload.
					rateLimiter.acquire();
					CreateMultipartUploadResponse response = s3Client.createMultipartUpload(
							builder -> customizeCreateMultipartUploadRequest(builder.bucket(bucket).key(key)));
					uploadId = response.uploadId();
				}
			}
		}
		return uploadId;
	}

	/**
	 * @return s3 upload URI (example: "s3://mybucket/parquetfile.parquet")
	 */
	public String getS3UploadUri()
	{
		return "s3://" + bucket + "/" + key;
	}

	/**
	 * Hook for customizing the create-multipart-upload request.
	 * <p>
	 * Default implementation is a no-op. Subclasses may override to mutate the provided builder before the request is
	 * built.
	 *
	 * @param req the request builder to customize
	 */
	protected void customizeCreateMultipartUploadRequest(CreateMultipartUploadRequest.Builder req)
	{
		// do nothing - this is for a child class to customize
	}


	/**
	 * Hook for customizing the upload-part request.
	 * <p>
	 * Default implementation is a no-op. Subclasses may override to mutate the provided builder before the request is
	 * built.
	 *
	 * @param req the request builder to customize
	 */
	protected void customizeUploadPartRequest(UploadPartRequest.Builder req)
	{
		// do nothing - this is for a child class to customize
	}

	/**
	 * Hook for customizing the complete-multipart-upload request.
	 * <p>
	 * Default implementation is a no-op. Subclasses may override to mutate the provided builder before the request is
	 * built.
	 *
	 * @param req the request builder to customize
	 */
	protected void customizeCompleteMultipartUploadRequest(CompleteMultipartUploadRequest.Builder req)
	{
		// do nothing - this is for a child class to customize
	}

	/**
	 * Hook for customizing the abort-multipart-upload request.
	 * <p>
	 * Default implementation is a no-op. Subclasses may override to mutate the provided builder before the request is
	 * built.
	 *
	 * @param req the request builder to customize
	 */
	protected void customizeAbortMultipartUploadRequest(AbortMultipartUploadRequest.Builder req)
	{
		// do nothing - this is for a child class to customize
	}

	@Override
	public synchronized void close()
	{
		if (!this.isCancelled && !this.isDone)
		{
			// if we haven't finished the upload, we better abort it.
			abortUpload();
		}
	}
}
