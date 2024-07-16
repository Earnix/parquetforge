package com.earnix.parquet.columnar.columnchunk;

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

	static class ObjectIteratorWrapper<T> extends BaseIteratorWrapper implements NullableObjectIterator<T>
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
		public boolean mightBeNull()
		{
			return false;
		}

		@Override
		public boolean isNull()
		{
			return false;
		}
	}

	interface NullableIterator
	{
		/**
		 * Returns whether this iterator may sometimes return null
		 * 
		 * @return whether this iterator may sometimes return null
		 */
		boolean mightBeNull();

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

	interface NullableDoubleIterator extends NullableIterator
	{
		double getValue();
	}

	interface NullableIntegerIterator extends NullableIterator
	{
		int getValue();
	}

	public interface NullableLongIterator extends NullableIterator
	{
		long getValue();
	}

	interface NullableBooleanIterator extends NullableIterator
	{
		boolean getValue();
	}

	interface NullableObjectIterator<T> extends NullableIterator
	{
		T getValue();
	}
}
