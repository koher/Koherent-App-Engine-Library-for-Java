package test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.text.DecimalFormat;

public abstract class IOStreamTestCase extends ReaderWriterTestCase {
	protected abstract InputStream getInputStream(String name);

	protected abstract OutputStream getOutputStream(String name);

	@Override
	public Reader getReader(String name) {
		return new InputStreamReader(getInputStream(name));
	}

	@Override
	public Writer getWriter(String name) {
		return new OutputStreamWriter(getOutputStream(name));
	}

	public void testSimpleOperations() {
		try {
			OutputStream out = getOutputStream("simple_operations");
			if (out == null) {
				throw new IOException("Cannot get an output stream.");
			}

			out.write(2);
			out.write(new byte[] { 3, 5, 7 });
			out.write(new byte[] { 2, 3, 5, 7, 11, 13, 17, 19 }, 4, 3);
			out.flush();
			out.close();

			InputStream in = getInputStream("simple_operations");
			if (in == null) {
				throw new IOException("Cannot get an input stream.");
			}

			byte[] bytes = new byte[10];

			in.read(bytes, 0, 3);
			bytes[3] = (byte) in.read();

			assertEquals(3, in.available());
			assertEquals(2, in.skip(2));

			byte[] left = new byte[5];
			in.read(left);
			System.arraycopy(left, 0, bytes, 4, left.length);

			int i = 0;
			assertEquals(2, bytes[i++]);
			assertEquals(3, bytes[i++]);
			assertEquals(5, bytes[i++]);
			assertEquals(7, bytes[i++]);
			assertEquals(17, bytes[i++]);
			assertEquals(0, bytes[i++]);

			assertEquals(0, in.available());
			assertEquals(0, in.skip(10));
			assertEquals(-1, in.read(new byte[1]));
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue(e.toString(), false);
		}
	}

	private static final int REPEATATION = 1000000;

	public void testBigData() {
		try {
			OutputStream out = getOutputStream("simple_operations");
			if (out == null) {
				throw new IOException("Cannot get an output stream.");
			}

			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					out));

			DecimalFormat format = new DecimalFormat("000000");
			for (int i = 0; i < REPEATATION; i++) {
				writer.write(format.format(i));
				writer.newLine();
			}

			writer.flush();
			writer.close();

			InputStream in = getInputStream("simple_operations");
			if (in == null) {
				throw new IOException("Cannot get an input stream.");
			}

			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));

			String line;
			int i = 0;
			try {
				while ((line = reader.readLine()) != null) {
					assertEquals(format.format(i), line);
					i++;
				}
			} catch (IOException e) {
				System.err.println(i);
				e.printStackTrace();
			}

			in.close();
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue(e.toString(), false);
		}
	}
}
