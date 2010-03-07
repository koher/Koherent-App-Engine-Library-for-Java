package test;

import java.util.Map;

import org.koherent.collection.UpdatableMap;
import org.koherent.collection.appengine.CachedDatastoreMap;
import org.koherent.object.StringParser;

public class CachedDatastoreMapTestCase extends UpdatableMapTestCase {
	@Override
	public UpdatableMap<String, String> getStringToStringMap() {
		return new CachedDatastoreMap<String, String>(
				CachedDatastoreMapTestCase.class.getSimpleName(), StringParser
						.getInstance());
	}

	@Override
	public Map<Integer, Long> getIntegerToLongMap() {
		return new CachedDatastoreMap<Integer, Long>(
				CachedDatastoreMapTestCase.class.getSimpleName());
	}
}
