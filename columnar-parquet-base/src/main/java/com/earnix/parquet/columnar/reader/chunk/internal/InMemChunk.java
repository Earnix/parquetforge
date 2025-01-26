package com.earnix.parquet.columnar.reader.chunk.internal;


import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

/**
 * Contains everything necessary for decoding a column without duplication of column data
 */
public class InMemChunk implements Serializable
{
	// not serializable - so need to custom serialize.
	private transient ColumnDescriptor descriptor;

	// can't be final because of custom serializer
	private transient Dictionary dictionary;

	// we'll assume these are serializable
	private final List<Supplier<DataPage>> dataPages;
	private final long totalValues;
	private final long totalPageBytes;

	public InMemChunk(InMemChunkPageStore pageStore) throws IOException
	{
		this.descriptor = pageStore.getDescriptor();
		DictionaryPage dictionaryPage = pageStore.getDictionaryPage().get();
		this.dictionary = readDictionary(dictionaryPage);
		this.dataPages = pageStore.getDataPageList();
		this.totalValues = pageStore.getTotalValues();
		this.totalPageBytes = pageStore.getTotalPageBytes();
		assertSerializable();
	}

	private void assertSerializable()
	{
		for (Supplier<DataPage> dataPageSupplier : this.dataPages)
		{
			if (!(dataPageSupplier instanceof Serializable))
			{
				throw new IllegalStateException();
			}
		}
	}

	private Dictionary readDictionary(DictionaryPage dp) throws IOException
	{
		if (dp == null)
			return null;
		return dp.getEncoding().initDictionary(descriptor, dp);
	}

	public ColumnDescriptor getDescriptor()
	{
		return descriptor;
	}

	public Dictionary getDictionary()
	{
		return dictionary;
	}

	public PageReader getDataPages()
	{
		return new MemPageReader(null, new DataPageIterator(this.dataPages.iterator()), getTotalValues());
	}

	public long getTotalValues()
	{
		return totalValues;
	}

	/**
	 * A rough estimate of the memory footprint of this column chunk
	 *
	 * @return a rough estimate of the memory footprint of this column chunk
	 */
	public long estimatedMemoryFootprint()
	{
		return 100L + 100L * dataPages.size() + this.totalPageBytes;
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException
	{
		in.defaultReadObject();
		descriptor = readColumnDescriptor(in);

		boolean hasDict = in.readBoolean();
		if (hasDict)
		{
			Encoding enc = (Encoding) in.readObject();
			int maxId = in.readInt();
			byte[] dat = (byte[]) in.readObject();
			BytesInput bytesInput = BytesInput.from(dat);
			DictionaryPage page = new DictionaryPage(bytesInput, maxId + 1, enc);
			this.dictionary = readDictionary(page);
		}
	}

	private void writeObject(ObjectOutputStream out) throws IOException
	{
		out.defaultWriteObject();
		writeColumnDescriptor(out);

		boolean hasDict = dictionary != null;
		out.writeBoolean(hasDict);
		if (hasDict)
		{
			out.writeObject(dictionary.getEncoding());
			int maxId = dictionary.getMaxId();
			out.writeInt(maxId);
			int bytesNeeded = computeBytesNeeded(descriptor.getPrimitiveType(), dictionary);

			byte[] data = new byte[bytesNeeded];
			ByteBuffer bb = ByteBuffer.wrap(data);
			bb.order(ByteOrder.LITTLE_ENDIAN);

			for (int i = 0; i <= maxId; i++)
			{
				switch (descriptor.getPrimitiveType().getPrimitiveTypeName())
				{
					case FLOAT:
						bb.putFloat(dictionary.decodeToFloat(i));
						break;
					case DOUBLE:
						bb.putDouble(dictionary.decodeToDouble(i));
						break;
					case INT32:
						bb.putInt(dictionary.decodeToInt(i));
						break;
					case INT64:
						bb.putLong(dictionary.decodeToLong(i));
						break;
					case BINARY:
						bb.put(dictionary.decodeToBinary(i).getBytesUnsafe());
						break;
				}
			}
			if (bb.hasRemaining())
				throw new IllegalStateException();
			out.writeObject(data);
		}
	}

	private ColumnDescriptor readColumnDescriptor(ObjectInputStream in) throws IOException, ClassNotFoundException
	{
		Type.Repetition repetition = (Type.Repetition) in.readObject();
		int typeLength = in.readInt();
		PrimitiveType.PrimitiveTypeName primitiveTypeName = (PrimitiveType.PrimitiveTypeName) in.readObject();
		String name = (String) in.readObject();
		PrimitiveType primitiveType = new PrimitiveType(repetition, primitiveTypeName, typeLength, name);

		String[] path = (String[]) in.readObject();
		int maxRep = in.readInt();
		int maxDef = in.readInt();
		return new ColumnDescriptor(path, primitiveType, maxRep, maxDef);
	}

	private void writeColumnDescriptor(ObjectOutputStream out) throws IOException
	{
		// write primitive type
		PrimitiveType primitiveType = descriptor.getPrimitiveType();
		out.writeObject(primitiveType.getRepetition());
		out.writeInt(primitiveType.getTypeLength());
		out.writeObject(primitiveType.getPrimitiveTypeName());
		out.writeObject(primitiveType.getName());

		out.writeObject(descriptor.getPath());
		out.writeInt(descriptor.getMaxRepetitionLevel());
		out.writeInt(descriptor.getMaxDefinitionLevel());
	}

	private static int computeBytesNeeded(PrimitiveType type, Dictionary dict)
	{
		int bytesNeeded;
		if (type.getPrimitiveTypeName() == BINARY)
		{
			bytesNeeded = 0;
			for (int i = 0; i <= dict.getMaxId(); i++)
			{
				// use add exact so overflow throws an exception.
				bytesNeeded = Math.addExact(bytesNeeded, Integer.BYTES);
				bytesNeeded = Math.addExact(bytesNeeded, dict.decodeToBinary(i).length());
			}
		}
		else
		{
			int elementSize = getElementSize(type);
			bytesNeeded = (dict.getMaxId() + 1) * elementSize;
		}
		return bytesNeeded;
	}

	private static int getElementSize(PrimitiveType type)
	{
		switch (type.getPrimitiveTypeName())
		{
			case FLOAT:
				return Float.BYTES;
			case INT32:
				return Integer.BYTES;
			case DOUBLE:
				return Double.BYTES;
			case INT64:
				return Long.BYTES;
			case FIXED_LEN_BYTE_ARRAY:
				return type.getTypeLength();
			case BINARY:
				return -1;
			// we don't support INT96, and boolean should not use dictionary.
			default:
				throw new IllegalStateException(type.toString());
		}
	}
}
