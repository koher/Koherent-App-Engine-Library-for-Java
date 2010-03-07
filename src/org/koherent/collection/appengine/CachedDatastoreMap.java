/*
 * Copyright 2010 the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 */

package org.koherent.collection.appengine;

import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.Set;

import org.koherent.collection.Updater;
import org.koherent.object.Parser;

import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.memcache.Expiration;

/**
 * A wrapper class of Datastore used on Google App Engine for Java caching
 * objects using Memcache transparently. It is able to operate Datastore as
 * <tt>java.util.Map</tt> using this class because it implements
 * <tt>java.util.Map</tt>.
 * 
 * <p>
 * Keys of each entry are converted to <tt>String</tt> using their
 * <tt>toString()</tt> methods. Values returned by <tt>toString()</tt> methods
 * must be <i>unique</i>: they must be equal when two objects are equal tested
 * by <tt>equals()</tt> methods or must not.
 * </p>
 * 
 * <p>
 * Stringified keys are parsed in {@link CachedDatastoreMap#keySet()} method and
 * {@link CachedDatastoreMap#entrySet()} method. Give a <tt>Parser</tt> object
 * to a constructor to support these methods. If a <tt>Parser</tt> is null or
 * not given, <tt>UnsupportedOperationException</tt> is thrown.
 * </p>
 * 
 * <p>
 * {@link DatastoreMap} is also available, which never cache objects.
 * </p>
 * 
 * @param <K>
 *            the type of keys maintained by this map
 * @param <V>
 *            the type of mapped values
 * 
 * @author koher
 * @version 0.1
 * @since 0.1
 * @see Parser
 * @see DatastoreMap
 * @see MemcacheMap
 */
public class CachedDatastoreMap<K, V> extends DatastoreMap<K, V> {
	protected MemcacheMap<K, V> memcacheMap;

	public CachedDatastoreMap(String kind) throws IllegalArgumentException {
		this(kind, null, DEFAULT_NUMBER_OF_RETRIES, null);
	}

	public CachedDatastoreMap(String kind, Parser<K> keyParser)
			throws IllegalArgumentException {
		this(kind, keyParser, DEFAULT_NUMBER_OF_RETRIES, null);
	}

	public CachedDatastoreMap(String kind, int numberOfRetries)
			throws IllegalArgumentException {
		this(kind, null, numberOfRetries, null);
	}

	public CachedDatastoreMap(String kind, Expiration expiration)
			throws IllegalArgumentException {
		this(kind, null, DEFAULT_NUMBER_OF_RETRIES, expiration);
	}

	public CachedDatastoreMap(String kind, Parser<K> keyParser,
			int numberOfRetries) throws IllegalArgumentException {
		this(kind, keyParser, numberOfRetries, null);
	}

	public CachedDatastoreMap(String kind, Parser<K> keyParser,
			int numberOfRetries, Expiration expiration)
			throws IllegalArgumentException {
		super(kind, keyParser, numberOfRetries);

		memcacheMap = new MemcacheMap<K, V>(kind, expiration);
	}

	@Override
	public void clear() {
		super.clear();
		memcacheMap.clear();
	}

	@Override
	public boolean containsKey(Object key) {
		return memcacheMap.containsKey(key) || super.containsKey(key);
	}

	/**
	 * @return a set view of the mappings contained in this map
	 * @throws UnsupportedOperationException
	 *             if <tt>keyParser</tt> is null or not given
	 */
	@Override
	public Set<Entry<K, V>> entrySet() throws UnsupportedOperationException {
		return new EntrySet();
	}

	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		V value;

		if (memcacheMap.containsKey(key)) {
			return memcacheMap.get(key);
		}

		try {
			value = createValue(service.get(createDatastoreKey(key)));
		} catch (EntityNotFoundException e) {
			return null;
		}

		try {
			memcacheMap.put((K) key, value);
		} catch (ClassCastException e) {
		}

		return value;
	}

	@Override
	public V put(K key, V value) throws ConcurrentModificationException {
		V oldValue = super.put(key, value);
		memcacheMap.remove(key);

		return oldValue;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		super.putAll(m);
		for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
			memcacheMap.remove(entry.getKey());
		}
	}

	@Override
	public V remove(Object key) throws ConcurrentModificationException {
		V value = super.remove(key);
		memcacheMap.remove(key);

		return value;
	}

	@Override
	public V update(K key, Updater<V> updater)
			throws ConcurrentModificationException {
		V value = super.update(key, updater);
		memcacheMap.remove(key);

		return value;
	}

	protected class EntrySet extends DatastoreMap<K, V>.EntrySet {
		@SuppressWarnings("unchecked")
		@Override
		public boolean contains(Object o) {
			if (!(o instanceof Entry)) {
				return false;
			}

			Entry<K, V> entry = (Entry<K, V>) o;
			V value = memcacheMap.get(entry.getKey());
			if (value == null) {
				try {
					value = getOrNotFound(entry.getKey());
				} catch (EntityNotFoundException e) {
					return false;
				}
			}

			return (value != null && value.equals(entry.getValue()))
					|| (value == null && entry.getValue() == null);
		}

		@SuppressWarnings("unchecked")
		@Override
		public boolean remove(Object o) {
			if (!(o instanceof Entry)) {
				return false;
			}

			Entry<K, V> entry = (Entry<K, V>) o;

			Transaction transaction = service.beginTransaction();

			V value;
			try {
				value = getOrNotFound(entry.getKey());
			} catch (EntityNotFoundException e) {
				transaction.rollback();

				return false;
			}

			if ((value != null && value.equals(entry.getValue()))
					|| (value == null && entry.getValue() == null)) {
				service.delete(createDatastoreKey(entry.getKey()));
				transaction.commit();
				memcacheMap.remove(entry.getKey());

				return true;
			} else {
				transaction.rollback();

				return false;
			}
		}
	}
}
