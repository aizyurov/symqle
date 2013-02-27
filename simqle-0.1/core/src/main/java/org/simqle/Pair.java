package org.simqle;

/**
 * Created by IntelliJ IDEA.
 * User: lvovich
 * Date: 11.10.11
 * Time: 15:43
 * To change this template use File | Settings | File Templates.
 */
public class Pair<First,Second> {
    private final First first;
    private final Second second;

    public Pair(final First first, final Second second) {
        this.first = first;
        this.second = second;
    }

    public First getFirst() {
        return first;
    }

    public Second getSecond() {
        return second;
    }

    public static <First,Second> Pair<First,Second> of(final First first, final Second second) {
        return new Pair<First, Second>(first, second);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair pair = (Pair) o;

        if (first != null ? !first.equals(pair.first) : pair.first != null) return false;
        if (second != null ? !second.equals(pair.second) : pair.second != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;
        result = 31 * result + (second != null ? second.hashCode() : 0);
        return result;
    }

    public String toString() {
        return "("+getFirst()+", "+getSecond()+")";
    }
}
