package test;

import java.io.CharArrayReader;
import java.io.CharArrayWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

public class CharArrayReaderWriterTestCase extends ReaderWriterTestCase {
	private Map<String, char[]> charArrayMap = new HashMap<String, char[]>();

	@Override
	public Reader getReader(String name) {
		return new CharArrayReader(charArrayMap.get(name));
	}

	@Override
	public Writer getWriter(String name) {
		final String NAME = name;

		return new CharArrayWriter() {
			@Override
			public void close() {
				charArrayMap.put(NAME, toCharArray());
				super.close();
			}
		};
	}
}
