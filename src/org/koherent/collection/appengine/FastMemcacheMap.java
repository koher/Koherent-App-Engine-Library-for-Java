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

import com.google.appengine.api.memcache.Expiration;

/**
 * This class provides faster ways to operate Memcache used on Google App Engine
 * for Java than {@link MemcacheMap}, which is achieved at the expense of the
 * complete support of <tt>java.util.Map</tt>.
 * 
 * <p>
 * {@link FastMemcacheMap#put()} and {@link FastMemcacheMap#remove()} always
 * return <tt>null</tt> while {@link java.util.Map#put()} and
 * {@link java.util.Map#remove()} return old values.
 * </p>
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
 * @since 0.2
 * @see MemcacheMap
 * @see FastDatastoreMap
 * @see FastCachedDatastoreMap
 */
public class FastMemcacheMap<K, V> extends MemcacheMap<K, V> {
	public FastMemcacheMap() {
		super();
	}

	public FastMemcacheMap(Expiration expiration) {
		super(expiration);
	}

	public FastMemcacheMap(String namespace) {
		super(namespace);
	}

	public FastMemcacheMap(String namespace, Expiration expiration) {
		super(namespace, expiration);
	}

	@Override
	public V put(K key, V value) {
		service.put(key, value, expiration);

		return null;
	}

	@Override
	public V remove(Object key) {
		service.delete(key);

		return null;
	}
}