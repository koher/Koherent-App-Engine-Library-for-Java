package test;

import java.util.Map;

import org.koherent.collection.UpdatableMap;
import org.koherent.collection.appengine.FastCachedDatastoreMap;
import org.koherent.object.StringParser;

public class FastCachedDatastoreMapTestCase extends FastUpdatableMapTestCase {
	@Override
	public UpdatableMap<String, String> getStringToStringMap() {
		return new FastCachedDatastoreMap<String, String>(
				FastCachedDatastoreMapTestCase.class.getName()
						+ "#getStringToStringMap", StringParser.getInstance());
	}

	@Override
	public Map<Integer, Long> getIntegerToLongMap() {
		return new FastCachedDatastoreMap<Integer, Long>(
				FastCachedDatastoreMapTestCase.class.getName()
						+ "#getIntegerToLongMap");
	}
}
