package com.earnix.parquet.columar.s3;

public class S3Constants
{
	// S3 parts must be at least this size (except for the last part)
	static final long MIN_S3_PART_SIZE = 5 * 1024 * 1024;
}
