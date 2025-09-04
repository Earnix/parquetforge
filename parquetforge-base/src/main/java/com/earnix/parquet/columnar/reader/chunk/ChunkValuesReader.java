package com.earnix.parquet.columnar.reader.chunk;

import org.apache.parquet.io.api.Binary;

/**
 * A reader for chunk values. Note that te
 */
public interface ChunkValuesReader
{
	/**
	 * @return Iterate to the next value
	 */
	boolean next();

	/**
	 * Skip the specified number of rows
	 *
	 * @param rowsToSkip number of rows to skip
	 */
	void skip(int rowsToSkip);

	/**
	 * @return whether the current value is null
	 */
	boolean isNull();

	int getInteger();

	boolean getBoolean();

	long getLong();

	Binary getBinary();

	float getFloat();

	double getDouble();

	/**
	 * @return whether dictionary id is supported
	 */
	boolean isDictionaryIdSupported();

	/**
	 * @return the id of the current element in the dictionary page. If the page encoding does not support dictionary an
	 * 		exception will be thrown.
	 */
	int getDictionaryId();
}
