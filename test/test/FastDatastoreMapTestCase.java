package test;

import java.util.Map;

import org.koherent.collection.UpdatableMap;
import org.koherent.collection.appengine.FastDatastoreMap;
import org.koherent.object.StringParser;

public class FastDatastoreMapTestCase extends FastUpdatableMapTestCase {
	@Override
	public UpdatableMap<String, String> getStringToStringMap() {
		return new FastDatastoreMap<String, String>(
				FastDatastoreMapTestCase.class.getName()
						+ "#getStringToStringMap", StringParser.getInstance());
	}

	@Override
	public Map<Integer, Long> getIntegerToLongMap() {
		return new FastDatastoreMap<Integer, Long>(
				FastDatastoreMapTestCase.class.getName()
						+ "#getStringToStringMap");
	}
}
