package test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.koherent.io.appengine.DatastoreInputStream;
import org.koherent.io.appengine.DatastoreOutputStream;
import org.koherent.io.appengine.WriteLockException;

public class DatastoreIOStreamTestCase extends IOStreamTestCase {
	@Override
	protected InputStream getInputStream(String name) {
		return new DatastoreInputStream(getName(name));
	}

	@Override
	protected OutputStream getOutputStream(String name) {
		try {
			return new DatastoreOutputStream(getName(name));
		} catch (WriteLockException e) {
			e.printStackTrace();

			return null;
		}
	}

	public void testAppend() {
		try {
			{
				OutputStream out = new DatastoreOutputStream(getName("append"));

				out.write(new byte[] { 1, 2, 3, 4, 5 });

				out.flush();
				out.close();
			}
			{
				OutputStream out = new DatastoreOutputStream(getName("append"),
						true);

				out.write(new byte[] { 6, 7, 8 });

				out.flush();
				out.close();
			}

			InputStream in = new DatastoreInputStream(getName("append"));

			assertEquals(1, in.read());
			assertEquals(2, in.read());
			assertEquals(3, in.read());
			assertEquals(4, in.read());
			assertEquals(5, in.read());
			assertEquals(6, in.read());
			assertEquals(7, in.read());
			assertEquals(8, in.read());
			assertEquals(-1, in.read());

			in.close();
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue(e.toString(), false);
		}
	}

	public void testLock() {
		OutputStream out1 = null;
		OutputStream out2 = null;
		try {
			out1 = new DatastoreOutputStream(getName("lock"));
			out2 = new DatastoreOutputStream(getName("lock"));

			assertTrue("A write-lock does not work.", false);
		} catch (WriteLockException e) {
			assertTrue("A write-lock works correctly.", true);
		} finally {
			if (out1 != null) {
				try {
					out1.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (out2 != null) {
				try {
					out2.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private String getName(String name) {
		return getClass().getName() + "." + name;
	}
}
