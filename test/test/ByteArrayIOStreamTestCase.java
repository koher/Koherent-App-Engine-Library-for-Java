package test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class ByteArrayIOStreamTestCase extends IOStreamTestCase {
	private Map<String, byte[]> byteMaps = new HashMap<String, byte[]>();

	@Override
	protected InputStream getInputStream(String name) {
		return new ByteArrayInputStream(byteMaps.get(name));
	}

	@Override
	protected OutputStream getOutputStream(String name) {
		final String NAME = name;

		return new ByteArrayOutputStream() {
			@Override
			public void close() throws IOException {
				byteMaps.put(NAME, this.toByteArray());
				super.close();
			}
		};
	}
}
