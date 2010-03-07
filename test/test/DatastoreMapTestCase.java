package test;

import java.util.Map;

import org.koherent.collection.UpdatableMap;
import org.koherent.collection.appengine.DatastoreMap;
import org.koherent.object.StringParser;

public class DatastoreMapTestCase extends UpdatableMapTestCase {
	@Override
	public UpdatableMap<String, String> getStringToStringMap() {
		return new DatastoreMap<String, String>(DatastoreMapTestCase.class
				.getSimpleName(), StringParser.getInstance());
	}

	@Override
	public Map<Integer, Long> getIntegerToLongMap() {
		return new DatastoreMap<Integer, Long>(DatastoreMapTestCase.class
				.getSimpleName());
	}
}
