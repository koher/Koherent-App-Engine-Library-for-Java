package test;

import java.util.HashMap;
import java.util.Map;

public abstract class FastMapTestCase extends MapTestCase {
	public void testSimpleOperations() {
		Map<String, String> map = getStringToStringMap();
		map.clear();

		// put

		assertEquals(null, map.put("abc", "111"));

		assertEquals("111", map.get("abc"));
		assertTrue(map.containsKey("abc"));

		assertEquals(null, map.put("abc", "123"));

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
		assertEquals(null, map.remove("aaa"));

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

	public void testIntegerToLongMap() {
		Map<Integer, Long> map = getIntegerToLongMap();
		map.clear();

		assertEquals(null, map.put(2, 1L));
		assertEquals(new Long(1L), map.get(2));
		assertTrue(map.containsKey(2));
		assertFalse(map.containsKey(11));
		assertEquals(null, map.put(2, 100L));
		assertTrue(map.containsKey(2));

		map.put(5, 200L);
		map.put(3, 300L);
		map.put(11, 400L);
		map.put(7, 500L);

		assertEquals(new Long(400L), map.get(11));
		assertTrue(map.containsKey(11));
		assertEquals(null, map.remove(11));
		assertEquals(null, map.get(11));
		assertFalse(map.containsKey(11));
		assertEquals(null, map.remove(11));
		assertFalse(map.containsKey(11));
	}
}
