package com.earnix.parquet.columnar.s3.buffering;

import com.earnix.parquet.columnar.s3.S3Constants;

import java.util.Arrays;

public class UploadPartUtils
{
	/**
	 * @param totalNumParts         number of parts we want to split this into.
	 * @param possibleUploadOffsets offsets that we can split the file at.
	 * @return the divisions (will be totalNumParts or less)
	 */
	static long[] computePartDivisions(int totalNumParts, long[] possibleUploadOffsets)
	{
		if (possibleUploadOffsets[0] != 0)
			throw new IllegalArgumentException("Starting offset must be zero..");

		int lastPossibleOffsetIdx = lastElementIdx(possibleUploadOffsets);

		// cannot upload anything because we have less data than the min s3 upload size. Note that uploading exactly
		// the min part size is acceptable
		if (belowS3MinPartSize(possibleUploadOffsets[lastPossibleOffsetIdx]))
			return null;

		long minSpace = computeMinSpace(totalNumParts, possibleUploadOffsets);

		// overallocate. Shrink at end.
		long[] offsets = new long[totalNumParts + 1];
		offsets[0] = 0L;

		int lastInPossibleOffsets = 0;
		for (int currOffset = 1; ; currOffset++)
		{
			lastInPossibleOffsets = findNextPartPlace(possibleUploadOffsets, lastInPossibleOffsets, minSpace);
			offsets[currOffset] = possibleUploadOffsets[lastInPossibleOffsets];
			if (lastInPossibleOffsets == lastPossibleOffsetIdx)
				return Arrays.copyOf(offsets, currOffset + 1);
		}
	}

	public static boolean belowS3MinPartSize(long size)
	{
		return size < S3Constants.MIN_S3_PART_SIZE;
	}

	private static int lastElementIdx(long[] possibleUploadOffsets)
	{
		return possibleUploadOffsets.length - 1;
	}

	private static int findNextPartPlace(long[] possibleUploadOffsets, int lastOffset, long targetSpace)
	{
		long targetEnd = possibleUploadOffsets[lastOffset] + targetSpace;
		int nextPartPlace = Arrays.binarySearch(possibleUploadOffsets, lastOffset, possibleUploadOffsets.length,
				targetEnd);

		// put the next place at the range of the element above the search key, if it was not exactly found
		if (nextPartPlace < 0)
		{
			nextPartPlace = Math.min(lastElementIdx(possibleUploadOffsets), -nextPartPlace - 1);
		}

		// make sure that whatever is leftover is at least minSpace.
		if (belowS3MinPartSize(
				possibleUploadOffsets[lastElementIdx(possibleUploadOffsets)] - possibleUploadOffsets[nextPartPlace]))
		{
			return lastElementIdx(possibleUploadOffsets);
		}

		return nextPartPlace;
	}

	private static long computeMinSpace(int totalNumParts, long[] possibleUploadOffsets)
	{
		long minSpace = ceilDiv(possibleUploadOffsets[lastElementIdx(possibleUploadOffsets)], totalNumParts);
		minSpace = Math.max(minSpace, S3Constants.MIN_S3_PART_SIZE);
		return minSpace;
	}

	private static long ceilDiv(long dividend, int divisor)
	{
		long minSpace = (dividend + divisor - 1) / divisor;
		return minSpace;
	}
}
