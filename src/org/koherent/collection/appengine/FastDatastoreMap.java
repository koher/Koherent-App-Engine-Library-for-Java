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
import java.util.Set;

import org.koherent.object.Parser;

/**
 * This class provides faster ways to operate Datastore used on Google App
 * Engine for Java than {@link DatastoreMap}, which is achieved at the expense
 * of the complete support of <tt>java.util.Map</tt>.
 * 
 * <p>
 * {@link FastDatastoreMap#put()} and {@link FastDatastoreMap#remove()} always
 * return <tt>null</tt> while {@link java.util.Map#put()} and
 * {@link java.util.Map#remove()} return old values.
 * </p>
 * 
 * <p>
 * See {@link DatastoreMap} to know how to use <tt>FastDatastoreMap</tt>.
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
 * @see DatastoreMap
 * @see Parser
 * @see FastMemcacheMap
 * @see FastCachedDatastoreMap
 */
public class FastDatastoreMap<K, V> extends DatastoreMap<K, V> {
	public FastDatastoreMap(String kind) throws IllegalArgumentException {
		super(kind);
	}

	public FastDatastoreMap(String kind, Parser<K> keyParser)
			throws IllegalArgumentException {
		super(kind, keyParser);
	}

	public FastDatastoreMap(String kind, int numberOfRetries)
			throws IllegalArgumentException {
		super(kind, numberOfRetries);
	}

	public FastDatastoreMap(String kind, Parser<K> keyParser,
			int numberOfRetries) throws IllegalArgumentException {
		super(kind, keyParser, numberOfRetries);
	}

	/**
	 * @return a set view of the mappings contained in this map
	 * @throws UnsupportedOperationException
	 *             if <tt>keyParser</tt> is null or not given
	 */
	@Override
	public Set<Entry<K, V>> entrySet() throws UnsupportedOperationException {
		if (keyParser == null) {
			throw new UnsupportedOperationException(
					"Give a Parser object to parse stringified keys.");
		}

		return new EntrySet();
	}

	@Override
	public V put(K key, V value) throws ConcurrentModificationException {
		service.put(createEntity(key, value));

		return null;
	}

	@Override
	public V remove(Object key) throws ConcurrentModificationException {
		service.delete(createDatastoreKey(key));

		return null;
	}
}
