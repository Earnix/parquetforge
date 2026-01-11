package com.earnix.parquet.columnar.reader.chunk;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;

/**
 * A reader for chunk values.
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

	Dictionary getDictionary();

	/**
	 * @return whether dictionary id is supported. Note that this must be checked every record. As a column chunk can
	 * 		contain pages that use a dictionary and pages that do not . See <a
	 * 		href="https://issues.apache.org/jira/browse/PARQUET-2221">here<a> and <a
	 * 		href="https://github.com/apache/parquet-format/issues/404">here</a>
	 */
	boolean isDictionaryIdSupported();

	/**
	 * Get the dictionary id of the current value if it is supported. Note that you MUST call
	 * {@link #isDictionaryIdSupported()} before each call to this. The parquet specification (as far as I can tell)
	 * does not require that all pages in a Column Chunk use the Dictionary even if it is there. See <a
	 * href="https://issues.apache.org/jira/browse/PARQUET-2221">here<a> and <a
	 * href="https://github.com/apache/parquet-format/issues/404">here</a>
	 *
	 * @return the id of the current element in the dictionary page. If the page encoding does not support dictionary an
	 * 		exception will be thrown.
	 */
	int getDictionaryId();
}
