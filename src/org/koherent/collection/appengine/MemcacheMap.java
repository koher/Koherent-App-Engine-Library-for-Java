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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.appengine.api.memcache.Expiration;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;

/**
 * A wrapper class of Memcache used on Google App Engine for Java. It is able to
 * operate Memcache like java.util.Map using this class because it implements
 * java.util.Map.
 * 
 * <p>
 * This class does not support some methods because of the functionality of
 * Memcache. <tt>UnsupportedOperationException</tt> is thrown when these methods
 * are called.
 * </p>
 * 
 * @param <K>
 *            the type of keys maintained by this map
 * @param <V>
 *            the type of mapped values
 * 
 * @author koher
 * @version 0.2
 * @since 0.1
 * @see DatastoreMap
 * @see CachedDatastoreMap
 */
public class MemcacheMap<K, V> implements Map<K, V> {
	protected MemcacheService service;
	protected Expiration expiration;

	public MemcacheMap() {
		this(null, null);
	}

	public MemcacheMap(Expiration expiration) {
		this(null, expiration);
	}

	public MemcacheMap(String namespace) {
		this(namespace, null);
	}

	public MemcacheMap(String namespace, Expiration expiration) {
		this.service = MemcacheServiceFactory.getMemcacheService();
		this.service.setNamespace(namespace);
		this.expiration = expiration;
	}

	@Override
	public void clear() {
		service.clearAll();
	}

	@Override
	public boolean containsKey(Object key) {
		return service.contains(key);
	}

	/**
	 * Unsupported.
	 * 
	 * @param value
	 * @return never returned
	 * @throws UnsupportedOperationException
	 *             always
	 */
	@Override
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Unsupported.
	 * 
	 * @return never returned
	 * @throws UnsupportedOperationException
	 *             always
	 */
	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		try {
			return (V) service.get(key);
		} catch (ClassCastException e) {
			return null;
		}
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	/**
	 * Unsupported.
	 * 
	 * @return never returned
	 * @throws UnsupportedOperationException
	 *             always
	 */
	@Override
	public Set<K> keySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public V put(K key, V value) {
		V oldValue = get(key);
		service.put(key, value, expiration);

		return oldValue;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		service.putAll((Map<Object, Object>) m, expiration);
	}

	@Override
	public V remove(Object key) {
		V value = get(key);
		service.delete(key);

		return value;
	}

	@Override
	public int size() {
		return (int) service.getStatistics().getItemCount();
	}

	/**
	 * Unsupported.
	 * 
	 * @return never returned
	 * @throws UnsupportedOperationException
	 *             always
	 */
	@Override
	public Collection<V> values() {
		throw new UnsupportedOperationException();
	}
}