package org.symqle.common;

/**
 * Base class to define 'strict equality'.
 * It means that {@code this.equals(other)} is false of other has different class.
 * Usage: {@code MyClass implements StrictEqual<MyClass>}, like Comparable.
 * Implementation must override {@link #hashCode()} to be consistent with
 * {@link #equalsTo(Object)}
 * @param <T> the type to which {@code this} may be equal.
 * @author lvovich
 */
public abstract class StrictEqual<T> {

    @Override
    @SuppressWarnings("unchecked")
    public final boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        return obj != null && obj.getClass() == getClass() && equalsTo((T) obj);
    }

    /**
     * Indicates that {@code other} is equal to {@code this}.
     * @param other guaranteed not null
     * @return {@code true} if {@code other} is equal to {@code this}
     */
    protected abstract boolean equalsTo(final T other);

    @Override
    public abstract int hashCode();

    /**
     * Useful for comparison of nullable class members in derived classes.
     * @param left first object to compare for equality
     * @param right second object to compare for equality
     * @param <X> type of compared objects
     * @return true if both null or left.equals(right)
     */
    protected final <X> boolean areSame(final X left, final X right) {
        return left != null ? left.equals(right) : right == null;
    }


}
