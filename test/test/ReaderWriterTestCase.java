package test;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

import base.LocalDatastoreTestCase;

public abstract class ReaderWriterTestCase extends LocalDatastoreTestCase {
	public abstract Reader getReader(String name);

	public abstract Writer getWriter(String name);

	public void testSimpleReadAndWrite() {
		try {
			Writer writer = null;
			try {
				writer = getWriter("simple_read_and_write");

				writer.write("abcdefg");
				writer.write("abcdefghijklmnopqrstuvwxyz", 7, 14);

				writer.flush();
			} finally {
				if (writer != null) {
					writer.close();
				}
			}

			Reader reader = null;
			try {
				reader = getReader("simple_read_and_write");

				int i;

				assertTrue(reader.ready());

				char[] chars1 = new char[10];
				assertEquals(10, reader.read(chars1));

				i = 0;
				assertEquals('a', chars1[i++]);
				assertEquals('b', chars1[i++]);
				assertEquals('c', chars1[i++]);
				assertEquals('d', chars1[i++]);
				assertEquals('e', chars1[i++]);
				assertEquals('f', chars1[i++]);
				assertEquals('g', chars1[i++]);
				assertEquals('h', chars1[i++]);
				assertEquals('i', chars1[i++]);
				assertEquals('j', chars1[i++]);

				char[] chars2 = new char[26];
				assertEquals(5, reader.read(chars2, 10, 5));

				i = 9;
				assertEquals('\0', chars2[i++]);
				assertEquals('k', chars2[i++]);
				assertEquals('l', chars2[i++]);
				assertEquals('m', chars2[i++]);
				assertEquals('n', chars2[i++]);
				assertEquals('o', chars2[i++]);
				assertEquals('\0', chars2[i++]);

				assertEquals('p', reader.read());

				char[] chars3 = new char[10];
				assertEquals(5, reader.read(chars3));

				i = 0;
				assertEquals('q', chars3[i++]);
				assertEquals('r', chars3[i++]);
				assertEquals('s', chars3[i++]);
				assertEquals('t', chars3[i++]);
				assertEquals('u', chars3[i++]);
				assertEquals('\0', chars3[i++]);

				assertFalse(reader.ready());
			} finally {
				if (reader != null) {
					reader.close();
				}
			}
		} catch (IOException e) {
			assertTrue(e.getMessage(), false);
		}
	}
}