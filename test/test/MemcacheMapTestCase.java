package test;

import java.util.Map;

import org.koherent.collection.appengine.MemcacheMap;

public class MemcacheMapTestCase extends MapTestCase {
	@Override
	public Map<String, String> getStringToStringMap() {
		return new MemcacheMap<String, String>(MemcacheMapTestCase.class
				.getName()
				+ "#getStringToStringMap");
	}

	@Override
	public Map<Integer, Long> getIntegerToLongMap() {
		return new MemcacheMap<Integer, Long>(MemcacheMapTestCase.class
				.getName()
				+ "#getIntegerToLongMap");
	}

	public void testNamespases() {
		Map<String, String> map1a = new MemcacheMap<String, String>("map1");
		Map<String, String> map1b = new MemcacheMap<String, String>("map1");
		Map<String, String> map2 = new MemcacheMap<String, String>("map2");

		map1a.put("abc", "123");
		map2.put("abc", "456");

		assertEquals("123", map1a.get("abc"));
		assertEquals("123", map1b.get("abc"));
		assertEquals("456", map2.get("abc"));
	}
}
