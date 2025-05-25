package com.earnix.parquet.columnar.writer.columnchunk;

import java.util.Iterator;
import java.util.PrimitiveIterator;

public class NullableIterators
{
	static <T> ObjectIteratorWrapper<T> wrapStringIterator(Iterator<T> it)
	{
		return new ObjectIteratorWrapper<>(it);
	}

	static ObjectIteratorWrapper<Boolean> wrapBooleanIterator(Iterator<Boolean> it)
	{
		return new ObjectIteratorWrapper<>(it);
	}

	static class ObjectIteratorWrapper<T> implements NullableObjectIterator<T>
	{
		private final Iterator<T> it;
		private T val;

		public ObjectIteratorWrapper(Iterator<T> it)
		{
			this.it = it;
		}

		@Override
		public T getValue()
		{
			return val;
		}

		@Override
		public boolean next()
		{
			if (it.hasNext())
			{
				val = it.next();
				return true;
			}
			return false;
		}

		@Override
		public boolean isNull()
		{
			return val == null;
		}
	}

	static NullableLongIterator wrapLongIterator(PrimitiveIterator.OfLong it)
	{
		return new LongIteratorWrapper(it);
	}

	static class LongIteratorWrapper extends BaseIteratorWrapper implements NullableLongIterator
	{
		private final PrimitiveIterator.OfLong it;
		private long val;

		public LongIteratorWrapper(PrimitiveIterator.OfLong it)
		{
			this.it = it;
		}

		@Override
		public long getValue()
		{
			return val;
		}

		@Override
		public boolean next()
		{
			if (it.hasNext())
			{
				val = it.nextLong();
				return true;
			}
			return false;
		}
	}

	static NullableIntegerIterator wrapIntegerIterator(PrimitiveIterator.OfInt it)
	{
		return new IntegerIteratorWrapper(it);
	}

	static class IntegerIteratorWrapper extends BaseIteratorWrapper implements NullableIntegerIterator
	{
		private final PrimitiveIterator.OfInt it;
		private int val;

		public IntegerIteratorWrapper(PrimitiveIterator.OfInt it)
		{
			this.it = it;
		}

		@Override
		public int getValue()
		{
			return val;
		}

		@Override
		public boolean next()
		{
			if (it.hasNext())
			{
				val = it.nextInt();
				return true;
			}
			return false;
		}
	}

	static NullableDoubleIterator wrapDoubleIterator(PrimitiveIterator.OfDouble it)
	{
		return new DoubleIteratorWrapper(it);
	}

	static class DoubleIteratorWrapper extends BaseIteratorWrapper implements NullableDoubleIterator
	{
		private final PrimitiveIterator.OfDouble it;
		private double val;

		public DoubleIteratorWrapper(PrimitiveIterator.OfDouble it)
		{
			this.it = it;
		}

		@Override
		public double getValue()
		{
			return val;
		}

		@Override
		public boolean next()
		{
			if (it.hasNext())
			{
				val = it.nextDouble();
				return true;
			}
			return false;
		}
	}

	static abstract class BaseIteratorWrapper implements NullableIterator
	{
		@Override
		public boolean isNull()
		{
			return false;
		}
	}

	public interface NullableIterator
	{
		/**
		 * Returns whether the current value is null
		 *
		 * @return whether the current value is null
		 */
		boolean isNull();

		/**
		 * Iterator to the next element
		 *
		 * @return whether the next element exists
		 */
		boolean next();
	}

	public interface NullableDoubleIterator extends NullableIterator
	{
		double getValue();
	}

	public interface NullableIntegerIterator extends NullableIterator
	{
		int getValue();
	}

	public interface NullableLongIterator extends NullableIterator
	{
		long getValue();
	}

	public interface NullableObjectIterator<T> extends NullableIterator
	{
		T getValue();
	}
}
