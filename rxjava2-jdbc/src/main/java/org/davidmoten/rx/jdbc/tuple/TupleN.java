package org.davidmoten.rx.jdbc.tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Variable length tuple backed by a List.
 * 
 * @param <T>
 *            type of each element of the tuple
 */
public class TupleN<T> {

    private final List<T> list;

    /**
     * Constructor.
     * 
     * @param list
     *            values of the elements in the tuple
     */
    public TupleN(List<T> list) {
        this.list = list;
    }

    @SafeVarargs
    public static <T> TupleN<T> create(T... array) {
        return new TupleN<T>(Arrays.asList(array));
    }

    public List<T> values() {
        // defensive copy
        return new ArrayList<T>(list);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((list == null) ? 0 : list.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TupleN<?> other = (TupleN<?>) obj;
        if (list == null) {
            if (other.list != null)
                return false;
        } else if (!list.equals(other.list))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "TupleN [values=" + list + "]";
    }

}
