package org.simqle;

/**
 * Created by IntelliJ IDEA.
 * User: lvovich
 * Date: 11.10.11
 * Time: 15:43
 * To change this template use File | Settings | File Templates.
 */
public class Pair<First, Second> {
    private final First _1;
    private final Second _2;

    public Pair(final First first, final Second second) {
        this._1 = first;
        this._2 = second;
    }

    public First first() {
        return _1;
    }

    public Second second() {
        return _2;
    }

    public static <First,Second> Pair<First,Second> make(final First first, final Second second) {
        return new Pair<First, Second>(first, second);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair pair = (Pair) o;

        if (_1 != null ? !_1.equals(pair._1) : pair._1 != null) return false;
        if (_2 != null ? !_2.equals(pair._2) : pair._2 != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = _1 != null ? _1.hashCode() : 0;
        result = 31 * result + (_2 != null ? _2.hashCode() : 0);
        return result;
    }

    public String toString() {
        return "("+ first()+", "+ second()+")";
    }
}
