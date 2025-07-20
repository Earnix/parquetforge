package com.earnix.parquet.columnar.reader.chunk;

import org.apache.parquet.io.api.Binary;

public interface ChunkValuesReader
{
	boolean isNull();

	boolean next();

	void skip(int rowsToSkip);

	int getInteger();

	boolean getBoolean();

	long getLong();

	Binary getBinary();

	float getFloat();

	double getDouble();
}
