package test;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

import org.koherent.io.appengine.DatastoreReader;
import org.koherent.io.appengine.DatastoreWriter;
import org.koherent.io.appengine.WriteLockException;

public class DatastoreReaderWriterTestCase extends ReaderWriterTestCase {
	@Override
	public Reader getReader(String name) {
		return new DatastoreReader(getName(name));
	}

	@Override
	public Writer getWriter(String name) {
		try {
			return new DatastoreWriter(getName(name));
		} catch (WriteLockException e) {
			return null;
		}
	}

	public void testAppend() {
		try {
			Writer writer = null;

			try {
				writer = new DatastoreWriter(getName("append"));

				writer.write("abc");

				writer.flush();
			} finally {
				if (writer != null) {
					writer.close();
				}
			}

			try {
				writer = new DatastoreWriter(getName("append"), true);

				writer.write("def");

				writer.flush();
			} finally {
				if (writer != null) {
					writer.close();
				}
			}

			Reader reader = null;

			try {
				reader = new DatastoreReader(getName("append"));

				char[] chars = new char[7];
				reader.read(chars);

				int i = 0;
				assertEquals('a', chars[i++]);
				assertEquals('b', chars[i++]);
				assertEquals('c', chars[i++]);
				assertEquals('d', chars[i++]);
				assertEquals('e', chars[i++]);
				assertEquals('f', chars[i++]);
				assertEquals('\0', chars[i++]);
			} finally {
				if (reader != null) {
					reader.close();
				}
			}
		} catch (IOException e) {
			assertTrue(e.getMessage(), false);
		}
	}

	private String getName(String name) {
		return getClass().getName() + "." + name;
	}
}
