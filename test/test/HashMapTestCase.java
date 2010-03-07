package test;

import java.util.HashMap;
import java.util.Map;

public class HashMapTestCase extends MapTestCase {
	@Override
	public Map<String, String> getStringToStringMap() {
		return new HashMap<String, String>();
	}

	@Override
	public Map<Integer, Long> getIntegerToLongMap() {
		return new HashMap<Integer, Long>();
	}
}
