package test;

import org.koherent.collection.UpdatableMap;
import org.koherent.collection.Updater;

public abstract class UpdatableMapTestCase extends MapTestCase {
	@Override
	public abstract UpdatableMap<String, String> getStringToStringMap();

	public void testUpdate() {
		UpdatableMap<String, String> map = getStringToStringMap();

		map.clear();

		map.put("abc", "111");
		map.put("def", "222");

		map.update("abc", new Updater<String>() {
			@Override
			public String update(String object) {
				return object + "999";
			}
		});

		assertEquals("111999", map.get("abc"));
		assertEquals("222", map.get("def"));
	}
}
