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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.koherent.collection.UpdatableMap;
import org.koherent.collection.Updater;
import org.koherent.object.Parser;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.DatastoreTimeoutException;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;

/**
 * A wrapper class of Datastore used on Google App Engine for Java. It is able
 * to operate Datastore as <tt>java.util.Map</tt> using this class because it
 * implements <tt>java.util.Map</tt>.
 * 
 * <p>
 * Keys of each entry are converted to <tt>String</tt> using their
 * <tt>toString()</tt> methods. Values returned by <tt>toString()</tt> methods
 * must be <i>unique</i>: they must be equal when two objects are equal tested
 * by <tt>equals()</tt> methods or must not.
 * </p>
 * 
 * <p>
 * Stringified keys are parsed in {@link DatastoreMap#keySet()} method and
 * {@link DatastoreMap#entrySet()} method. Give a <tt>Parser</tt> object to a
 * constructor to support these methods. If a <tt>Parser</tt> is null or not
 * given, <tt>UnsupportedOperationException</tt> is thrown.
 * </p>
 * 
 * <p>
 * {@link CachedDatastoreMap} is also available, which automatically cache
 * objects using Memcache.
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
 * @see MemcacheMap
 * @see CachedDatastoreMap
 */
public class DatastoreMap<K, V> implements UpdatableMap<K, V> {
	public static final int DEFAULT_NUMBER_OF_RETRIES = 4;

	protected static final String PROPERTY_NAME = "value";
	protected static final int MAX_NUMBER_OF_ENTITIES_TO_DELETE_ONCE = 500;

	protected DatastoreService service;
	protected String kind;
	protected Parser<K> keyParser;
	protected int numberOfRetries;

	public DatastoreMap(String kind) throws IllegalArgumentException {
		this(kind, null, DEFAULT_NUMBER_OF_RETRIES);
	}

	public DatastoreMap(String kind, Parser<K> keyParser)
			throws IllegalArgumentException {
		this(kind, keyParser, DEFAULT_NUMBER_OF_RETRIES);
	}

	public DatastoreMap(String kind, int numberOfRetries)
			throws IllegalArgumentException {
		this(kind, null, numberOfRetries);
	}

	public DatastoreMap(String kind, Parser<K> keyParser, int numberOfRetries)
			throws IllegalArgumentException {
		if (kind == null) {
			throw new IllegalArgumentException("\"kind\" cannot be null.");
		}

		this.service = DatastoreServiceFactory.getDatastoreService();
		this.kind = kind;
		this.keyParser = keyParser;
		this.numberOfRetries = DEFAULT_NUMBER_OF_RETRIES;
	}

	protected Key createDatastoreKey(Object key) throws NullPointerException {
		if (key == null) {
			return KeyFactory.createKey(kind, 1L);
		}

		return KeyFactory.createKey(kind, key.toString());
	}

	protected Entity createEntity(K key, V value) throws NullPointerException {
		return createEntity(createDatastoreKey(key), value);
	}

	protected Entity createEntity(Key datastoreKey, V value) {
		Entity entity = new Entity(datastoreKey);
		if (value == null) {
			entity.setProperty(PROPERTY_NAME, null);
			return entity;
		}

		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		try {
			ObjectOutputStream objectOut = new ObjectOutputStream(byteOut);
			objectOut.writeObject(value);

			Blob blob = new Blob(byteOut.toByteArray());
			entity.setProperty(PROPERTY_NAME, blob);

			// no need to close ByteArrayOutputStreams
		} catch (IOException e) {
			throw new IllegalArgumentException(
					"Cannot seriarize the given value.");
		}

		return entity;
	}

	protected K createKey(Entity entity) {
		return keyParser.parse(entity.getKey().getName());
	}

	@SuppressWarnings("unchecked")
	protected V createValue(Entity entity) {
		if (entity == null) {
			return null;
		}

		try {
			Blob blob = (Blob) entity.getProperty(PROPERTY_NAME);
			if (blob == null) {
				return null;
			}

			ByteArrayInputStream byteIn = new ByteArrayInputStream(blob
					.getBytes());
			ObjectInputStream objectIn = new ObjectInputStream(byteIn);

			return (V) objectIn.readObject();

			// no need to close ByteArrayInputStreams
		} catch (ClassCastException e) {
			return null;
		} catch (IOException e) {
			return null;
		} catch (ClassNotFoundException e) {
			return null;
		}
	}

	protected PreparedQuery getPreparedQueryForAllEntities(boolean keysOnly) {
		Query query = new Query(kind);
		if (keysOnly) {
			query = query.setKeysOnly();
		}
		return service.prepare(query);
	}

	protected Iterable<Entity> getAllEntitiesAsIterable(boolean keysOnly) {
		return getPreparedQueryForAllEntities(keysOnly).asIterable();
	}

	protected Iterator<Entity> getAllEntitiesAsIterator(boolean keysOnly) {
		return getPreparedQueryForAllEntities(keysOnly).asIterator();
	}

	protected List<Entity> getAllEntitiesAsList(boolean keysOnly) {
		return getPreparedQueryForAllEntities(keysOnly).asList(
				FetchOptions.Builder.withOffset(0));
	}

	@Override
	public void clear() {
		List<Key> keys = new ArrayList<Key>(
				MAX_NUMBER_OF_ENTITIES_TO_DELETE_ONCE);
		Iterable<Entity> entities = getAllEntitiesAsIterable(true);
		for (Entity entity : entities) {
			keys.add(entity.getKey());

			if (keys.size() == MAX_NUMBER_OF_ENTITIES_TO_DELETE_ONCE) {
				try {
					service.delete(keys);
				} catch (DatastoreTimeoutException e) {
				}

				keys.clear();
			}
		}
		if (keys.size() > 0) {
			service.delete(keys);
		}
	}

	@Override
	public boolean containsKey(Object key) {
		try {
			service.get(createDatastoreKey(key));

			return true;
		} catch (EntityNotFoundException e) {
			return false;
		}
	}

	@Override
	public boolean containsValue(Object value) {
		Iterable<Entity> entities = getAllEntitiesAsIterable(false);

		for (Entity entity : entities) {
			V v = createValue(entity);
			if (v != null && v.equals(value)) {
				return true;
			}
		}

		return false;
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
	public V get(Object key) {
		try {
			return getOrNotFound(key);
		} catch (EntityNotFoundException e) {
			return null;
		}
	}

	protected V getOrNotFound(Object key) throws EntityNotFoundException {
		return createValue(service.get(createDatastoreKey(key)));
	}

	@Override
	public boolean isEmpty() {
		Iterable<Entity> entities = getAllEntitiesAsIterable(true);

		return !entities.iterator().hasNext();
	}

	/**
	 * @return a set view of the keys contained in this map
	 * @throws UnsupportedOperationException
	 *             if <tt>keyParser</tt> is null or not given
	 */
	@Override
	public Set<K> keySet() throws UnsupportedOperationException {
		if (keyParser == null) {
			throw new UnsupportedOperationException(
					"Give a Parser object to parse stringified keys.");
		}

		return new KeySet();
	}

	@Override
	public V put(K key, V value) throws ConcurrentModificationException {
		ConcurrentModificationException exception;
		int retryCount = 0;

		do {
			Transaction transaction = service.beginTransaction();

			V oldValue = get(key);

			try {
				service.put(createEntity(key, value));
				transaction.commit();

				return oldValue;
			} catch (ConcurrentModificationException e) {
				if (transaction.isActive()) {
					transaction.rollback();
				}

				exception = e;
			}
		} while (retryCount++ < numberOfRetries);

		throw exception;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		for (Entry<? extends K, ? extends V> entry : m.entrySet()) {
			service.put(createEntity(entry.getKey(), entry.getValue()));
		}
	}

	@Override
	public V remove(Object key) throws ConcurrentModificationException {
		ConcurrentModificationException exception;
		int retryCount = 0;

		do {
			Transaction transaction = service.beginTransaction();

			V oldValue = get(key);

			try {
				service.delete(createDatastoreKey(key));
				transaction.commit();

				return oldValue;
			} catch (ConcurrentModificationException e) {
				if (transaction.isActive()) {
					transaction.rollback();
				}

				exception = e;
			}
		} while (retryCount++ < numberOfRetries);

		throw exception;
	}

	@Override
	public int size() {
		return getPreparedQueryForAllEntities(true).countEntities();
	}

	@Override
	public V update(K key, Updater<V> updater)
			throws ConcurrentModificationException {
		ConcurrentModificationException exception;
		int retryCount = 0;

		do {
			Transaction transaction = service.beginTransaction();

			V value = null;
			try {
				value = getOrNotFound(key);
			} catch (EntityNotFoundException e) {
				transaction.rollback();

				return null;
			}

			value = updater.update(value);

			try {
				service.put(createEntity(key, value));
				transaction.commit();

				return value;
			} catch (ConcurrentModificationException e) {
				if (transaction.isActive()) {
					transaction.rollback();
				}

				exception = e;
			}
		} while (retryCount++ < numberOfRetries);

		throw exception;
	}

	@Override
	public Collection<V> values() {
		return new ValueCollection();
	}

	protected class DatastoreEntry implements Entry<K, V> {
		private K key;

		public DatastoreEntry(K key) {
			this.key = key;
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return DatastoreMap.this.get(key);
		}

		@Override
		public V setValue(V value) {
			return DatastoreMap.this.put(key, value);
		}

		@SuppressWarnings("unchecked")
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof Entry) {
				try {
					Entry<K, V> anotherEntry = (Entry<K, V>) obj;

					V value = getValue();

					if (key == null) {
						if (value == null) {
							return anotherEntry.getKey() == null
									&& anotherEntry.getValue() == null;
						} else {
							return anotherEntry.getKey() == null
									&& value.equals(anotherEntry.getValue());
						}
					} else {
						if (value == null) {
							return key.equals(anotherEntry.getKey())
									&& anotherEntry.getValue() == null;
						} else {
							return key.equals(anotherEntry.getKey())
									&& value.equals(anotherEntry.getValue());
						}
					}
				} catch (ClassCastException e) {
					return false;
				}
			}
			return false;
		}

		@Override
		public int hashCode() {
			V value = getValue();

			return (key != null ? key.hashCode() : 0)
					+ (value != null ? value.hashCode() : 0);
		}
	}

	protected class EntrySet extends AbstractCollection<Entry<K, V>> implements
			Set<Entry<K, V>> {
		@SuppressWarnings("unchecked")
		@Override
		public boolean contains(Object o) {
			if (!(o instanceof Entry)) {
				return false;
			}

			Entry<K, V> entry = (Entry<K, V>) o;
			V value;
			try {
				value = getOrNotFound(entry.getKey());
			} catch (EntityNotFoundException e) {
				return false;
			}

			return (value != null && value.equals(entry.getValue()))
					|| (value == null && entry.getValue() == null);
		}

		@Override
		public Iterator<Entry<K, V>> iterator() {
			return new Iterator<Entry<K, V>>() {
				private Iterator<Entity> iterator = DatastoreMap.this
						.getAllEntitiesAsIterator(true);
				private Entry<K, V> entry;

				@Override
				public boolean hasNext() {
					return iterator.hasNext();
				}

				@Override
				public Entry<K, V> next() {
					Entity entity = iterator.next();
					entry = new DatastoreEntry(createKey(entity));

					return entry;
				}

				@Override
				public void remove() {
					DatastoreMap.this.remove(entry.getKey());
				}
			};
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

				return true;
			} else {
				transaction.rollback();

				return false;
			}
		}

		@Override
		public boolean removeAll(Collection<?> c) {
			boolean modified = false;

			for (Object object : c) {
				modified |= remove(object);
			}

			return modified;
		}

		@Override
		public boolean retainAll(Collection<?> c) {
			boolean modified = false;

			Iterator<Entry<K, V>> iterator = iterator();

			while (iterator.hasNext()) {
				Entry<K, V> key = iterator.next();
				if (!c.contains(key)) {
					iterator.remove();
					modified = true;
				}
			}

			return modified;
		}

		@Override
		protected List<java.util.Map.Entry<K, V>> toList() {
			List<Entity> entities = getAllEntitiesAsList(true);
			List<Entry<K, V>> list = new ArrayList<Entry<K, V>>();
			for (Entity entity : entities) {
				list.add(new DatastoreEntry(createKey(entity)));
			}

			return list;
		}
	}

	protected class KeySet extends AbstractCollection<K> implements Set<K> {
		@Override
		public boolean contains(Object o) {
			return DatastoreMap.this.containsKey(o);
		}

		@Override
		public Iterator<K> iterator() {
			return new Iterator<K>() {
				private Iterator<Entity> iterator = getAllEntitiesAsIterator(true);
				private K key;

				@Override
				public boolean hasNext() {
					return iterator.hasNext();
				}

				@Override
				public K next() {
					key = createKey(iterator.next());

					return key;
				}

				@Override
				public void remove() {
					DatastoreMap.this.remove(key);
				}
			};
		}

		@Override
		public boolean remove(Object o) {
			return DatastoreMap.this.remove(o) != null;
		}

		@Override
		public boolean removeAll(Collection<?> c) {
			boolean modified = false;

			for (Object key : c) {
				modified |= remove(key);
			}

			return modified;
		}

		@Override
		public boolean retainAll(Collection<?> c) {
			boolean modified = false;

			Iterator<K> iterator = iterator();

			while (iterator.hasNext()) {
				K key = iterator.next();
				if (!c.contains(key)) {
					iterator.remove();
					modified = true;
				}
			}

			return modified;
		}

		protected List<K> toList() {
			List<Entity> entities = getAllEntitiesAsList(true);
			List<K> list = new ArrayList<K>();
			for (Entity entity : entities) {
				list.add(createKey(entity));
			}

			return list;
		}
	}

	protected class ValueCollection extends AbstractCollection<V> {
		@Override
		public boolean contains(Object o) {
			return DatastoreMap.this.containsValue(o);
		}

		@Override
		public Iterator<V> iterator() {
			return new Iterator<V>() {
				private Iterator<Entity> iterator = getAllEntitiesAsIterator(false);
				private K key;

				@Override
				public boolean hasNext() {
					return iterator.hasNext();
				}

				@Override
				public V next() {
					Entity entity = iterator.next();
					key = createKey(entity);

					return createValue(entity);
				}

				@Override
				public void remove() {
					DatastoreMap.this.remove(key);
				}
			};
		}

		@Override
		public boolean remove(Object o) {
			boolean modified = false;

			Iterator<V> iterator = iterator();

			while (iterator.hasNext()) {
				V value = iterator.next();
				if ((o == null && value == null)
						|| (o != null && o.equals(value))) {
					iterator.remove();
					modified = true;
				}
			}

			return modified;
		}

		@Override
		public boolean removeAll(Collection<?> c) {
			return removeOrRetainAll(c, true);
		}

		@Override
		public boolean retainAll(Collection<?> c) {
			return removeOrRetainAll(c, false);
		}

		protected boolean removeOrRetainAll(Collection<?> c, boolean removing) {
			boolean modified = false;

			Iterator<V> iterator = iterator();

			while (iterator.hasNext()) {
				V value = iterator.next();
				if (removing == c.contains(value)) {
					iterator.remove();
					modified = true;
				}
			}

			return modified;
		}

		protected List<V> toList() {
			List<Entity> entities = getAllEntitiesAsList(false);
			List<V> list = new ArrayList<V>();
			for (Entity entity : entities) {
				list.add(createValue(entity));
			}

			return list;
		}
	}

	protected abstract class AbstractCollection<E> implements Collection<E> {
		@Override
		public boolean add(E e) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean addAll(Collection<? extends E> c) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear() {
			DatastoreMap.this.clear();
		}

		@Override
		public boolean containsAll(Collection<?> c) {
			for (Object object : c) {
				if (!contains(object)) {
					return false;
				}
			}

			return true;
		}

		@Override
		public boolean isEmpty() {
			return DatastoreMap.this.isEmpty();
		}

		@Override
		public int size() {
			return DatastoreMap.this.size();
		}

		@Override
		public Object[] toArray() {
			return toList().toArray();
		}

		@Override
		public <T> T[] toArray(T[] a) {
			return toList().toArray(a);
		}

		protected abstract List<E> toList();
	}
}
