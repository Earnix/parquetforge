package com.earnix.parquet.columnar.s3;

public class S3Constants
{
	// S3 parts must be at least this size (except for the last part)
	public static final long MIN_S3_PART_SIZE = 5 * 1024 * 1024;
	public static final int MAX_PARTS_PER_FILE = 10_000;
}
