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
 * This class provides faster ways to operate Datastore used on Google App
 * Engine for Java caching objects using Memcache transparently than
 * {@link CachedDatastoreMap}, which is achieved at the expense of the complete
 * support of <tt>java.util.Map</tt>.
 * 
 * <p>
 * {@link FastCachedDatastoreMap#put()} and {@link FastDatastoreMap#remove()}
 * always return <tt>null</tt> while {@link java.util.Map#put()} and
 * {@link java.util.Map#remove()} return old values.
 * </p>
 * 
 * <p>
 * See {@link CachedDatastoreMap} to know how to use
 * <tt>FastCachedDatastoreMap</tt>.
 * </p>
 * 
 * @param <K>
 *            the type of keys maintained by this map
 * @param <V>
 *            the type of mapped values
 * 
 * @author koher
 * @version 0.2
 * @since 0.2
 * @see CachedDatastoreMap
 * @see Parser
 * @see FastDatastoreMap
 * @see FastMemcacheMap
 */
public class FastCachedDatastoreMap<K, V> extends FastDatastoreMap<K, V> {
	protected FastMemcacheMap<K, V> memcacheMap;

	public FastCachedDatastoreMap(String kind) throws IllegalArgumentException {
		this(kind, null, DEFAULT_NUMBER_OF_RETRIES, null);
	}

	public FastCachedDatastoreMap(String kind, Parser<K> keyParser)
			throws IllegalArgumentException {
		this(kind, keyParser, DEFAULT_NUMBER_OF_RETRIES, null);
	}

	public FastCachedDatastoreMap(String kind, int numberOfRetries)
			throws IllegalArgumentException {
		this(kind, null, numberOfRetries, null);
	}

	public FastCachedDatastoreMap(String kind, Expiration expiration)
			throws IllegalArgumentException {
		this(kind, null, DEFAULT_NUMBER_OF_RETRIES, expiration);
	}

	public FastCachedDatastoreMap(String kind, Parser<K> keyParser,
			int numberOfRetries) throws IllegalArgumentException {
		this(kind, keyParser, numberOfRetries, null);
	}

	public FastCachedDatastoreMap(String kind, Parser<K> keyParser,
			int numberOfRetries, Expiration expiration)
			throws IllegalArgumentException {
		super(kind, keyParser, numberOfRetries);

		memcacheMap = new FastMemcacheMap<K, V>(kind, expiration);
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
		super.put(key, value);
		memcacheMap.remove(key);

		return null;
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
		super.remove(key);
		memcacheMap.remove(key);

		return null;
	}

	@Override
	public V update(K key, Updater<V> updater)
			throws ConcurrentModificationException {
		V value = super.update(key, updater);
		memcacheMap.remove(key);

		return value;
	}

	protected class EntrySet extends FastDatastoreMap<K, V>.EntrySet {
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
