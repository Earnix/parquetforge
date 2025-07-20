package com.earnix.parquet.columnar.s3.buffering;

import org.junit.Assert;
import org.junit.Test;

public class PartitioningTest
{
	private static final int FIVE_MB = 5 * 1024 * 1024;

	@Test
	public void testBasicScenario()
	{
		long[] possibleOffsets = { 0, FIVE_MB };

		long[] offsets = UploadPartUtils.computePartDivisions(1, possibleOffsets);
		Assert.assertArrayEquals(possibleOffsets, offsets);
	}

	@Test
	public void testBasicScenario2()
	{
		long[] possibleOffsets = { 0, FIVE_MB, FIVE_MB * 2 };

		long[] offsets = UploadPartUtils.computePartDivisions(2, possibleOffsets);
		Assert.assertArrayEquals(possibleOffsets, offsets);
	}

	@Test
	public void testBasicScenario3()
	{
		long[] possibleOffsets = { 0, FIVE_MB, 2 * FIVE_MB, 3 * FIVE_MB };

		long[] offsets = UploadPartUtils.computePartDivisions(3, possibleOffsets);
		Assert.assertArrayEquals(possibleOffsets, offsets);
	}

	@Test
	public void testUnevenScenario()
	{
		long[] possibleOffsets = { 0, FIVE_MB, 2 * FIVE_MB, 3 * FIVE_MB };

		long[] offsets = UploadPartUtils.computePartDivisions(2, possibleOffsets);
		Assert.assertArrayEquals(new long[] { 0, 2 * FIVE_MB, 3 * FIVE_MB }, offsets);
	}

	@Test
	public void testLessPartsThanRequested()
	{
		long[] possibleOffsets = { 0, FIVE_MB, FIVE_MB * 2 - 1 };

		long[] offsets = UploadPartUtils.computePartDivisions(2, possibleOffsets);
		Assert.assertArrayEquals(new long[] { 0, FIVE_MB * 2 - 1 }, offsets);
	}

	@Test
	public void testLessPartsThanRequested2()
	{
		long[] possibleOffsets = { 0, FIVE_MB - 1, FIVE_MB };

		long[] offsets = UploadPartUtils.computePartDivisions(2, possibleOffsets);
		Assert.assertArrayEquals(new long[] { 0, FIVE_MB }, offsets);
	}

	@Test
	public void testNotPossible()
	{
		Assert.assertNull(UploadPartUtils.computePartDivisions(2, new long[] { 0, FIVE_MB - 1 }));
		Assert.assertNull(UploadPartUtils.computePartDivisions(2, new long[] { 0, 3, FIVE_MB - 1 }));
		Assert.assertNull(UploadPartUtils.computePartDivisions(2, new long[] { 0, 3 }));
		Assert.assertNull(UploadPartUtils.computePartDivisions(2, new long[] { 0, 3, 3, 5, 6, 99 }));
	}
}
