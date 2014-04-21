/*
   Copyright 2010-2013 Alexander Izyurov

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.package org.symqle.common;
*/

package org.symqle.common;

/**
 * Pair of objects.
 * @param <First> type of first object
 * @param <Second> type of second object
 */
public class Pair<First, Second> {
    private final First o1;
    private final Second o2;

    /**
     * Constructs a pair from 2 objects.
     * @param first the first object
     * @param second the second object
     */
    public Pair(final First first, final Second second) {
        this.o1 = first;
        this.o2 = second;
    }

    /**
     * First member of pair.
     * @return the first member
     */
    public final First first() {
        return o1;
    }

    /**
     * Second member of pair.
     * @return the second member
     */
    public final Second second() {
        return o2;
    }

    /**
     * A factory method for constructing a pair from 2 objects.
     * @param first the first object
     * @param second the second object
     * @param <First> type of {@code first}
     * @param <Second> type of {@code second}
     * @return new Pair
     */
    public static <First, Second> Pair<First, Second> make(final First first, final Second second) {
        return new Pair<First, Second>(first, second);
    }

    @Override
    public final boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Pair pair = (Pair) o;

        if (o1 != null ? !o1.equals(pair.o1) : pair.o1 != null) {
            return false;
        }
        if (o2 != null ? !o2.equals(pair.o2) : pair.o2 != null) {
            return false;
        }
        return true;
    }

    @Override
    public final int hashCode() {
        int result = o1 != null ? o1.hashCode() : 0;
        result = 31 * result + (o2 != null ? o2.hashCode() : 0);
        return result;
    }

    public final String toString() {
        return "(" + first() + ", " + second() + ")";
    }
}
