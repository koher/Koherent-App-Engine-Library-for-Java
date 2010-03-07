package test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import base.LocalDatastoreTestCase;

public abstract class MapTestCase extends LocalDatastoreTestCase {
	public abstract Map<String, String> getStringToStringMap();

	public abstract Map<Integer, Long> getIntegerToLongMap();

	public void testSimpleOperations() {
		Map<String, String> map = getStringToStringMap();
		map.clear();

		// put

		assertEquals(null, map.put("abc", "111"));

		assertEquals("111", map.get("abc"));
		assertTrue(map.containsKey("abc"));

		assertEquals("111", map.put("abc", "123"));

		assertEquals("123", map.get("abc"));
		assertTrue(map.containsKey("abc"));
		try {
			assertTrue(map.containsValue("123"));
		} catch (UnsupportedOperationException e) {
		}

		assertEquals(null, map.get("def"));
		assertFalse(map.containsKey("def"));
		try {
			assertFalse(map.containsValue("456"));
		} catch (UnsupportedOperationException e) {
		}

		assertEquals(1, map.size());

		// putAll
		Map<String, String> inputMap = new HashMap<String, String>();
		inputMap.put("aaa", "987");
		inputMap.put("Z9.", "555");
		inputMap.put(" ^^", "   ");
		map.putAll(inputMap);

		assertEquals("987", map.get("aaa"));
		assertEquals("555", map.get("Z9."));
		assertEquals("   ", map.get(" ^^"));

		assertEquals(4, map.size());

		// remove
		assertEquals("987", map.remove("aaa"));

		assertEquals(null, map.get("aaa"));
		assertFalse(map.containsKey("aaa"));
		try {
			assertFalse(map.containsValue("987"));
		} catch (UnsupportedOperationException e) {
		}

		assertEquals(3, map.size());

		// clear
		map.clear();

		assertEquals(null, map.get("abc"));
		assertFalse(map.containsKey("abc"));
		try {
			assertFalse(map.containsValue("123"));
		} catch (UnsupportedOperationException e) {
		}

		assertEquals(0, map.size());

		// null
		map.put("zzz", null);
		map.put(null, "999");

		assertEquals(null, map.get("zzz"));
		assertTrue(map.containsKey("zzz"));
		assertEquals("999", map.get(null));
		assertTrue(map.containsKey(null));
		assertEquals(2, map.size());

		assertFalse(map.containsKey("aaa"));
		assertEquals(null, map.get("aaa"));
		assertFalse(map.containsKey("aaa"));
		assertEquals(null, map.remove("aaa"));
		assertFalse(map.containsKey("aaa"));
	}

	private static final int LARGE_NUMBER = 10000;

	public void testLargeNumberOfData() {
		Map<String, String> map = getStringToStringMap();
		map.clear();

		Map<String, String> inputMap = new HashMap<String, String>();
		for (int i = 0; i < LARGE_NUMBER; i++) {
			inputMap.put("key" + i, "value" + i);
		}

		map.putAll(inputMap);

		for (int i = 0; i < 100; i++) {
			int j = (int) (LARGE_NUMBER * Math.random());
			assertEquals("value" + j, map.get("key" + j));
		}
	}

	public void testOperationsThroughCollections() {
		Map<String, String> inputMap = new HashMap<String, String>();
		inputMap.put("abc", "123");
		inputMap.put("def", "456");
		inputMap.put("ghi", "789");
		inputMap.put("jkl", "000");
		inputMap.put("mno", "999");
		inputMap.put("pqr", "555");

		Map<String, String> map = getStringToStringMap();
		map.clear();

		int size;

		// keySet
		try {
			map.putAll(inputMap);
			size = inputMap.size();

			Set<String> keySet = map.keySet();

			assertEquals(size, keySet.size());

			Iterator<String> iterator = keySet.iterator();
			while (iterator.hasNext()) {
				String key = iterator.next();

				if ("def".equals(key)) {
					iterator.remove();
				}
			}

			assertTrue(keySet.contains("abc"));
			assertFalse(keySet.contains("def"));
			assertEquals(--size, keySet.size());

			keySet.remove("pqr");

			assertTrue(keySet.contains("mno"));
			assertFalse(keySet.contains("pqr"));
			assertEquals(--size, keySet.size());

			keySet.removeAll(Arrays.asList(new String[] { "ghi", "mno" }));
			assertTrue(keySet.containsAll(Arrays.asList(new String[] { "abc",
					"jkl" })));
			assertFalse(keySet.contains("ghi"));
			assertFalse(keySet.contains("mno"));
			assertEquals(size -= 2, keySet.size());

			keySet.clear();

			assertEquals(0, keySet.size());
		} catch (UnsupportedOperationException e) {
		}

		// valueCollection
		try {
			map.putAll(inputMap);
			size = inputMap.size();

			Collection<String> values = map.values();

			assertEquals(size, values.size());

			Iterator<String> iterator = values.iterator();
			while (iterator.hasNext()) {
				String key = iterator.next();

				if ("456".equals(key)) {
					iterator.remove();
				}
			}

			assertTrue(values.contains("123"));
			assertFalse(values.contains("456"));
			assertEquals(--size, values.size());

			values.remove("555");

			assertTrue(values.contains("999"));
			assertFalse(values.contains("555"));
			assertEquals(--size, values.size());

			values.removeAll(Arrays.asList(new String[] { "789", "000" }));
			assertTrue(values.containsAll(Arrays.asList(new String[] { "123",
					"999" })));
			assertFalse(values.contains("789"));
			assertFalse(values.contains("000"));
			assertEquals(size -= 2, values.size());

			values.clear();

			assertEquals(0, values.size());
		} catch (UnsupportedOperationException e) {
		}

		// entrySet
		// keySet
		try {
			map.putAll(inputMap);
			size = inputMap.size();

			Set<Entry<String, String>> entrySet = map.entrySet();

			assertEquals(size, entrySet.size());

			Iterator<Entry<String, String>> iterator = entrySet.iterator();
			while (iterator.hasNext()) {
				Entry<String, String> entry = iterator.next();

				if ("def".equals(entry.getKey())) {
					iterator.remove();
				}
			}

			assertTrue(entrySet.contains(new SimpleEntry<String, String>("abc",
					"123")));
			assertFalse(entrySet.contains(new SimpleEntry<String, String>(
					"abc", "111"))); // invalid Entry => not contained
			assertFalse(entrySet.contains(new SimpleEntry<String, String>(
					"def", "456")));
			assertEquals(--size, entrySet.size());

			map.get("pqr"); // cache
			entrySet.remove(new SimpleEntry<String, String>("pqr", "555")); // remove the cached entry
			entrySet.remove(new SimpleEntry<String, String>("abc", "222")); // invalid Entry => no effect

			assertTrue(map.containsKey("abc")); // test that the entry is not removed
			assertTrue(entrySet.contains(new SimpleEntry<String, String>("mno",
					"999")));
			assertFalse(entrySet.contains(new SimpleEntry<String, String>(
					"pqr", "555")));
			assertFalse(map.containsKey("pqr")); // test that the cache is removed
			assertEquals(--size, entrySet.size());

			entrySet.removeAll(Arrays.asList(new Entry[] {
					new SimpleEntry<String, String>("ghi", "789"),
					new SimpleEntry<String, String>("mno", "999") }));
			assertTrue(entrySet.containsAll(Arrays.asList(new Entry[] {
					new SimpleEntry<String, String>("abc", "123"),
					new SimpleEntry<String, String>("jkl", "000") })));
			assertFalse(entrySet.contains(new SimpleEntry<String, String>(
					"ghi", "789")));
			assertFalse(entrySet.contains(new SimpleEntry<String, String>(
					"mno", "999")));
			assertEquals(size -= 2, entrySet.size());

			entrySet.clear();

			assertEquals(0, entrySet.size());
		} catch (UnsupportedOperationException e) {
		}
	}

	public void testIntegerToLongMap() {
		Map<Integer, Long> map = getIntegerToLongMap();
		map.clear();

		assertEquals(null, map.put(2, 1L));
		assertEquals(new Long(1L), map.get(2));
		assertTrue(map.containsKey(2));
		assertFalse(map.containsKey(11));
		assertEquals(new Long(1L), map.put(2, 100L));
		assertTrue(map.containsKey(2));

		map.put(5, 200L);
		map.put(3, 300L);
		map.put(11, 400L);
		map.put(7, 500L);

		assertEquals(new Long(400L), map.get(11));
		assertTrue(map.containsKey(11));
		assertEquals(new Long(400L), map.remove(11));
		assertEquals(null, map.get(11));
		assertFalse(map.containsKey(11));
		assertEquals(null, map.remove(11));
		assertFalse(map.containsKey(11));
	}

	protected class SimpleEntry<K, V> implements Entry<K, V> {
		private K key;
		private V value;

		public SimpleEntry(K key, V value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public V setValue(V value) {
			V oldValue = this.value;
			this.value = value;

			return oldValue;
		}

		@SuppressWarnings("unchecked")
		@Override
		public boolean equals(Object obj) {
			if (obj instanceof Entry) {
				try {
					Entry<K, V> anotherEntry = (Entry<K, V>) obj;

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
			return (key != null ? key.hashCode() : 0)
					+ (value != null ? value.hashCode() : 0);
		}
	}
}
