/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.server.search.util.gnu.trove;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;


//////////////////////////////////////////////////
// THIS IS A GENERATED CLASS. DO NOT HAND EDIT! //
//////////////////////////////////////////////////


/**
 * An open addressed Map implementation for int keys and Object values.
 * <p/>
 * Created: Sun Nov  4 08:52:45 2001
 *
 * @author Eric D. Friedman
 */
public class TIntObjectHashMap<V> extends TIntHash implements Externalizable {
    static final long serialVersionUID = 1L;

    private final TIntObjectProcedure<V> PUT_ALL_PROC = new TIntObjectProcedure<V>() {
        public boolean execute(int key, V value) {
            put(key, value);
            return true;
        }
    };


    /**
     * the values of the map
     */
    protected transient V[] _values;

    /**
     * Creates a new <code>TIntObjectHashMap</code> instance with the default
     * capacity and load factor.
     */
    public TIntObjectHashMap() {
        super();
    }

    /**
     * Creates a new <code>TIntObjectHashMap</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the default load factor.
     *
     * @param initialCapacity an <code>int</code> value
     */
    public TIntObjectHashMap(int initialCapacity) {
        super(initialCapacity);
    }

    /**
     * Creates a new <code>TIntObjectHashMap</code> instance with a prime
     * capacity equal to or greater than <tt>initialCapacity</tt> and
     * with the specified load factor.
     *
     * @param initialCapacity an <code>int</code> value
     * @param loadFactor      a <code>float</code> value
     */
    public TIntObjectHashMap(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    /**
     * Creates a new <code>TIntObjectHashMap</code> instance with the default
     * capacity and load factor.
     *
     * @param strategy used to compute hash codes and to compare keys.
     */
    public TIntObjectHashMap(TIntHashingStrategy strategy) {
        super(strategy);
    }

    /**
     * Creates a new <code>TIntObjectHashMap</code> instance whose capacity
     * is the next highest prime above <tt>initialCapacity + 1</tt>
     * unless that value is already prime.
     *
     * @param initialCapacity an <code>int</code> value
     * @param strategy        used to compute hash codes and to compare keys.
     */
    public TIntObjectHashMap(int initialCapacity, TIntHashingStrategy strategy) {
        super(initialCapacity, strategy);
    }

    /**
     * Creates a new <code>TIntObjectHashMap</code> instance with a prime
     * value at or near the specified capacity and load factor.
     *
     * @param initialCapacity used to find a prime capacity for the table.
     * @param loadFactor      used to calculate the threshold over which
     *                        rehashing takes place.
     * @param strategy        used to compute hash codes and to compare keys.
     */
    public TIntObjectHashMap(int initialCapacity, float loadFactor, TIntHashingStrategy strategy) {
        super(initialCapacity, loadFactor, strategy);
    }

    /**
     * @return a deep clone of this collection
     */
    public TIntObjectHashMap<V> clone() {
        TIntObjectHashMap<V> m = (TIntObjectHashMap<V>) super.clone();
        m._values = (V[]) this._values.clone();
        return m;
    }

    /**
     * @return a TIntObjectIterator with access to this map's keys and values
     */
    public TIntObjectIterator<V> iterator() {
        return new TIntObjectIterator<V>(this);
    }

    /**
     * initializes the hashtable to a prime capacity which is at least
     * <tt>initialCapacity + 1</tt>.
     *
     * @param initialCapacity an <code>int</code> value
     * @return the actual capacity chosen
     */
    protected int setUp(int initialCapacity) {
        int capacity;

        capacity = super.setUp(initialCapacity);
        _values = (V[]) new Object[capacity];
        return capacity;
    }

    /**
     * Inserts a key/value pair into the map.
     *
     * @param key   an <code>int</code> value
     * @param value an <code>Object</code> value
     * @return the previous value associated with <tt>key</tt>,
     *         or {@code null} if none was found.
     */
    public V put(int key, V value) {
        int index = insertionIndex(key);
        return doPut(key, value, index);
    }

    /**
     * Inserts a key/value pair into the map if the specified key is not already
     * associated with a value.
     *
     * @param key   an <code>int</code> value
     * @param value an <code>Object</code> value
     * @return the previous value associated with <tt>key</tt>,
     *         or {@code null} if none was found.
     */
    public V putIfAbsent(int key, V value) {
        int index = insertionIndex(key);
        if (index < 0)
            return _values[-index - 1];
        return doPut(key, value, index);
    }

    private V doPut(int key, V value, int index) {
        byte previousState;
        V previous = null;
        boolean isNewMapping = true;
        if (index < 0) {
            index = -index - 1;
            previous = _values[index];
            isNewMapping = false;
        }
        previousState = _states[index];
        _set[index] = key;
        _states[index] = FULL;
        _values[index] = value;
        if (isNewMapping) {
            postInsertHook(previousState == FREE);
        }

        return previous;
    }


    /**
     * Put all the entries from the given map into this map.
     *
     * @param map The map from which entries will be obtained to put into this map.
     */
    public void putAll(TIntObjectHashMap<V> map) {
        map.forEachEntry(PUT_ALL_PROC);
    }


    /**
     * rehashes the map to the new capacity.
     *
     * @param newCapacity an <code>int</code> value
     */
    protected void rehash(int newCapacity) {
        int oldCapacity = _set.length;
        int oldKeys[] = _set;
        V oldVals[] = _values;
        byte oldStates[] = _states;

        _set = new int[newCapacity];
        _values = (V[]) new Object[newCapacity];
        _states = new byte[newCapacity];

        for (int i = oldCapacity; i-- > 0;) {
            if (oldStates[i] == FULL) {
                int o = oldKeys[i];
                int index = insertionIndex(o);
                _set[index] = o;
                _values[index] = oldVals[i];
                _states[index] = FULL;
            }
        }
    }

    /**
     * retrieves the value for <tt>key</tt>
     *
     * @param key an <code>int</code> value
     * @return the value of <tt>key</tt> or (int)0 if no such mapping exists.
     */
    public V get(int key) {
        int index = index(key);
        return index < 0 ? null : _values[index];
    }

    /**
     * Empties the map.
     */
    public void clear() {
        super.clear();
        int[] keys = _set;
        Object[] vals = _values;
        byte[] states = _states;

        Arrays.fill(_set, 0, _set.length, (int) 0);
        Arrays.fill(_values, 0, _values.length, null);
        Arrays.fill(_states, 0, _states.length, FREE);
    }

    /**
     * Deletes a key/value pair from the map.
     *
     * @param key an <code>int</code> value
     * @return an <code>Object</code> value or (int)0 if no such mapping exists.
     */
    public V remove(int key) {
        V prev = null;
        int index = index(key);
        if (index >= 0) {
            prev = _values[index];
            removeAt(index);    // clear key,state; adjust size
        }
        return prev;
    }

    /**
     * Compares this map with another map for equality of their stored
     * entries.
     *
     * @param other an <code>Object</code> value
     * @return a <code>boolean</code> value
     */
    public boolean equals(Object other) {
        if (!(other instanceof TIntObjectHashMap)) {
            return false;
        }
        TIntObjectHashMap that = (TIntObjectHashMap) other;
        if (that.size() != this.size()) {
            return false;
        }
        return forEachEntry(new EqProcedure(that));
    }

    public int hashCode() {
        HashProcedure p = new HashProcedure();
        forEachEntry(p);
        return p.getHashCode();
    }

    private final class HashProcedure implements TIntObjectProcedure {
        private int h = 0;

        public int getHashCode() {
            return h;
        }

        public final boolean execute(int key, Object value) {
            h += (_hashingStrategy.computeHashCode(key) ^ HashFunctions.hash(value));
            return true;
        }
    }

    private static final class EqProcedure implements TIntObjectProcedure {
        private final TIntObjectHashMap _otherMap;

        EqProcedure(TIntObjectHashMap otherMap) {
            _otherMap = otherMap;
        }

        public final boolean execute(int key, Object value) {
            int index = _otherMap.index(key);
            if (index >= 0 && eq(value, _otherMap.get(key))) {
                return true;
            }
            return false;
        }

        /**
         * Compare two objects for equality.
         */
        private final boolean eq(Object o1, Object o2) {
            return o1 == o2 || ((o1 != null) && o1.equals(o2));
        }

    }

    /**
     * removes the mapping at <tt>index</tt> from the map.
     *
     * @param index an <code>int</code> value
     */
    protected void removeAt(int index) {
        _values[index] = null;
        super.removeAt(index);  // clear key, state; adjust size
    }

    /**
     * Returns the values of the map.
     *
     * @return a <code>Collection</code> value
     * @see #getValues(Object[])
     */
    public Object[] getValues() {
        Object[] vals = new Object[size()];
        V[] v = _values;
        byte[] states = _states;

        for (int i = v.length, j = 0; i-- > 0;) {
            if (states[i] == FULL) {
                vals[j++] = v[i];
            }
        }
        return vals;
    }

    /**
     * Return the values of the map; the runtime type of the returned array is that of
     * the specified array.
     *
     * @param a the array into which the elements of this collection are to be
     *          stored, if it is big enough; otherwise, a new array of the same
     *          runtime type is allocated for this purpose.
     * @return an array containing the elements of this collection
     * @throws ArrayStoreException  the runtime type of the specified array is
     *                              not a supertype of the runtime type of every element in this
     *                              collection.
     * @throws NullPointerException if the specified array is <tt>null</tt>.
     * @see #getValues()
     */
    public <T> T[] getValues(T[] a) {
        if (a.length < _size) {
            a = (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(),
                    _size);
        }

        V[] v = _values;
        byte[] states = _states;

        for (int i = v.length, j = 0; i-- > 0;) {
            if (states[i] == FULL) {
                a[j++] = (T) v[i];
            }
        }
        return a;
    }

    /**
     * returns the keys of the map.
     *
     * @return a <code>Set</code> value
     */
    public int[] keys() {
        int[] keys = new int[size()];
        int[] k = _set;
        byte[] states = _states;

        for (int i = k.length, j = 0; i-- > 0;) {
            if (states[i] == FULL) {
                keys[j++] = k[i];
            }
        }
        return keys;
    }

    /**
     * returns the keys of the map.
     *
     * @param a the array into which the elements of the list are to
     *          be stored, if it is big enough; otherwise, a new array of the
     *          same type is allocated for this purpose.
     * @return a <code>Set</code> value
     */
    public int[] keys(int[] a) {
        int size = size();
        if (a.length < size) {
            a = (int[]) java.lang.reflect.Array.newInstance(
                    a.getClass().getComponentType(), size);
        }

        int[] k = (int[]) _set;
        byte[] states = _states;

        for (int i = k.length, j = 0; i-- > 0;) {
            if (states[i] == FULL) {
                a[j++] = k[i];
            }
        }
        return a;
    }

    /**
     * checks for the presence of <tt>val</tt> in the values of the map.
     *
     * @param val an <code>Object</code> value
     * @return a <code>boolean</code> value
     */
    public boolean containsValue(V val) {
        byte[] states = _states;
        V[] vals = _values;

        // special case null values so that we don't have to
        // perform null checks before every call to equals()
        if (null == val) {
            for (int i = vals.length; i-- > 0;) {
                if (states[i] == FULL &&
                        val == vals[i]) {
                    return true;
                }
            }
        } else {
            for (int i = vals.length; i-- > 0;) {
                if (states[i] == FULL &&
                        (val == vals[i] || val.equals(vals[i]))) {
                    return true;
                }
            }
        } // end of else
        return false;
    }


    /**
     * checks for the present of <tt>key</tt> in the keys of the map.
     *
     * @param key an <code>int</code> value
     * @return a <code>boolean</code> value
     */
    public boolean containsKey(int key) {
        return contains(key);
    }

    /**
     * Executes <tt>procedure</tt> for each key in the map.
     *
     * @param procedure a <code>TIntProcedure</code> value
     * @return false if the loop over the keys terminated because
     *         the procedure returned false for some key.
     */
    public boolean forEachKey(TIntProcedure procedure) {
        return forEach(procedure);
    }

    /**
     * Executes <tt>procedure</tt> for each value in the map.
     *
     * @param procedure a <code>TObjectProcedure</code> value
     * @return false if the loop over the values terminated because
     *         the procedure returned false for some value.
     */
    public boolean forEachValue(TObjectProcedure<V> procedure) {
        byte[] states = _states;
        V[] values = _values;
        for (int i = values.length; i-- > 0;) {
            if (states[i] == FULL && !procedure.execute(values[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Executes <tt>procedure</tt> for each key/value entry in the
     * map.
     *
     * @param procedure a <code>TOIntObjectProcedure</code> value
     * @return false if the loop over the entries terminated because
     *         the procedure returned false for some entry.
     */
    public boolean forEachEntry(TIntObjectProcedure<V> procedure) {
        byte[] states = _states;
        int[] keys = _set;
        V[] values = _values;
        for (int i = keys.length; i-- > 0;) {
            if (states[i] == FULL && !procedure.execute(keys[i], values[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Retains only those entries in the map for which the procedure
     * returns a true value.
     *
     * @param procedure determines which entries to keep
     * @return true if the map was modified.
     */
    public boolean retainEntries(TIntObjectProcedure<V> procedure) {
        boolean modified = false;
        byte[] states = _states;
        int[] keys = _set;
        V[] values = _values;

        // Temporarily disable compaction. This is a fix for bug #1738760
        tempDisableAutoCompaction();
        try {
            for (int i = keys.length; i-- > 0;) {
                if (states[i] == FULL && !procedure.execute(keys[i], values[i])) {
                    removeAt(i);
                    modified = true;
                }
            }
        }
        finally {
            reenableAutoCompaction(true);
        }

        return modified;
    }

    /**
     * Transform the values in this map using <tt>function</tt>.
     *
     * @param function a <code>TObjectFunction</code> value
     */
    public void transformValues(TObjectFunction<V, V> function) {
        byte[] states = _states;
        V[] values = _values;
        for (int i = values.length; i-- > 0;) {
            if (states[i] == FULL) {
                values[i] = function.execute(values[i]);
            }
        }
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        // VERSION
        out.writeByte(0);

        // NUMBER OF ENTRIES
        out.writeInt(_size);

        // ENTRIES
        SerializationProcedure writeProcedure = new SerializationProcedure(out);
        if (!forEachEntry(writeProcedure)) {
            throw writeProcedure.exception;
        }
    }

    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {

        // VERSION
        in.readByte();

        // NUMBER OF ENTRIES
        int size = in.readInt();
        setUp(size);

        // ENTRIES
        while (size-- > 0) {
            int key = in.readInt();
            V val = (V) in.readObject();
            put(key, val);
        }
    }

    public String toString() {
        final StringBuilder buf = new StringBuilder("{");
        forEachEntry(new TIntObjectProcedure<V>() {
            private boolean first = true;

            public boolean execute(int key, Object value) {
                if (first) first = false;
                else buf.append(",");

                buf.append(key);
                buf.append("=");
                buf.append(value);
                return true;
            }
        });
        buf.append("}");
        return buf.toString();
    }
} // TIntObjectHashMap
