/*
 * Copyright 2010 the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 */

package org.koherent.io.appengine;

import java.io.IOException;
import java.io.InputStream;

import org.koherent.io.appengine.DatastoreStreamingIO.Metadata;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;

/**
 * A <code>DatastoreInputStream</code> is an <code>InputStream</code> to read
 * data from a Datastore used on Google App Engine for Java.
 * <code>DatastoreInputStream</code> and <code>DatastoreOutputStream</code> can
 * be alternatives of <code>FileInputStream</code> and
 * <code>FileOutputStream</code>.
 * 
 * <p>
 * A <code>DatastoreInputStream</code> obtains input bytes in Datastore. It is
 * usual to use <code>DatastoreOutputStream</code> to write data which can be
 * read by <code>DatastoreInputStream</code>.
 * </p>
 * 
 * @author koher
 * @version 0.1
 * @since 0.1
 * @see DatastoreOutputStream
 * @see java.io.FileInputStream
 * @see java.io.InputStream
 */
public class DatastoreInputStream extends InputStream {
	public static final int BUFFER_SIZE = DatastoreStreamingIO.BUFFER_SIZE;

	private String kind;
	private long page;
	private byte[] buffer;
	private int pointer;

	private DatastoreService service;

	private Metadata metadata;

	public DatastoreInputStream(String kind) {
		this.kind = kind;

		page = 0L;
		pointer = 0;

		service = DatastoreServiceFactory.getDatastoreService();
		metadata = DatastoreStreamingIO.readMetadata(this.kind, service);
	}

	@Override
	public int available() throws IOException {
		if (!readBuffer()) {
			return 0;
		}

		int available;
		if (page == metadata.page) {
			available = buffer.length - pointer;
		} else {
			available = (int) (metadata.page - page) * BUFFER_SIZE
					+ (metadata.pointer - pointer);
		}
		if (available <= 0) {
			available = 0;
		}

		return available;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public int read() throws IOException {
		if (!readBuffer()) {
			return -1;
		}

		return buffer[pointer++] & 0xff;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if (b == null) {
			throw new NullPointerException();
		} else if (off < 0 || len < 0 || len > b.length - off) {
			throw new IndexOutOfBoundsException();
		}

		if (len <= 0) {
			return 0;
		}

		int readLen = 0;
		while (len > 0) {
			int currentPageLen;

			if (!readBuffer()) {
				break;
			}

			if (pointer + len > buffer.length) {
				currentPageLen = buffer.length - pointer;
			} else {
				currentPageLen = len;
			}
			len -= currentPageLen;

			if (currentPageLen <= 0) {
				continue;
			}

			System.arraycopy(buffer, pointer, b, off, currentPageLen);
			pointer += currentPageLen;
			off += currentPageLen;
			readLen += currentPageLen;
		}

		if (readLen == 0) {
			return -1;
		}

		return readLen;
	}

	@Override
	public long skip(long n) throws IOException {
		int skippedLen = 0;
		while (n > 0) {
			long currentPageLen;

			if (!readBuffer()) {
				break;
			}

			if (pointer + n > buffer.length) {
				currentPageLen = buffer.length - pointer;
			} else {
				currentPageLen = n;
			}
			n -= currentPageLen;

			if (currentPageLen <= 0) {
				continue;
			}

			pointer += currentPageLen;
			skippedLen += currentPageLen;
		}

		return skippedLen;
	}

	protected boolean readBuffer() {
		if (buffer != null && pointer >= buffer.length) {
			page++;
			pointer = 0;
		}

		while (pointer == 0) {
			try {
				Entity entity = service.get(KeyFactory.createKey(kind, Long
						.toHexString(page)));
				long version = (Long) entity
						.getProperty(DatastoreStreamingIO.DATA__PROPERTY__VERSION);

				long versionDifference = version - metadata.version;

				if (versionDifference == 0) {
					buffer = ((Blob) entity
							.getProperty(DatastoreStreamingIO.DATA__PROPERTY__DATA))
							.getBytes();

					if (buffer.length > 0) {
						break;
					}
				} else if (versionDifference > 0) {
					buffer = ((Blob) entity
							.getProperty(DatastoreStreamingIO.DATA__PROPERTY__DATA))
							.getBytes();

					metadata = DatastoreStreamingIO.readMetadata(kind, service);

					if (buffer.length > 0 && version == metadata.version) {
						break;
					}
				} else {
					return false;
				}
			} catch (EntityNotFoundException e) {
				return false;
			}
		}

		return true;
	}
}
