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
import java.io.OutputStream;
import java.util.ConcurrentModificationException;

import org.koherent.io.appengine.DatastoreStreamingIO.Metadata;
import org.koherent.math.Integers;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Transaction;

/**
 * A <code>DatastoreOutputStream</code> is an <code>OutputStream</code> to write
 * data to a Datastore used on Google App Engine for Java.
 * <code>DatastoreInputStream</code> and <code>DatastoreOutputStream</code> can
 * be alternatives of <code>FileInputStream</code> and
 * <code>FileOutputStream</code>.
 * 
 * <p>
 * <code>FileOutputStream</code> is not available on Google App Engine for Java.
 * <code>DatastoreOutputStream</code> enables write data to a Datastore in a
 * similar way with <code>FileOutputStream</code>.
 * </p>
 * 
 * <p>
 * Data written by <code>DatastoreOutputStream</code> can be read using a
 * <code>DatastoreInputStream</code>.
 * </p>
 * 
 * @author koher
 * @version 0.1
 * @since 0.1
 * @see DatastoreInputStream
 * @see java.io.FileOutputStream
 * @see java.io.OutputStream
 */
public class DatastoreOutputStream extends OutputStream {
	public static final int BUFFER_SIZE = DatastoreStreamingIO.BUFFER_SIZE;

	protected static final int INITIAL_BUFFER_SIZE = 10 * 1024; // 10 kilobytes

	protected static final long MAX_LOCK_TIME = 31 * 1000; // 31 seconds

	protected static final String META_DATA__KEY = DatastoreStreamingIO.META_DATA__KEY;
	protected static final String META_DATA__PROPERTY__NUMBER_OF_PAGES = DatastoreStreamingIO.META_DATA__PROPERTY__PAGE;
	protected static final String META_DATA__PROPERTY__DATA_SIZE_OF_FINAL_PAGE = DatastoreStreamingIO.META_DATA__PROPERTY__POINTER;

	protected static final String DATA__PROPERTY__DATA = DatastoreStreamingIO.DATA__PROPERTY__DATA;
	protected static final String DATA__PROPERTY__VERSION = DatastoreStreamingIO.DATA__PROPERTY__VERSION;

	private String kind;
	private long page;
	private byte[] buffer;
	private int pointer;

	private long lastFlushPage;
	private int lastFlushPointer;

	private DatastoreService service;

	private Metadata metadata;

	public DatastoreOutputStream(String kind, boolean append)
			throws WriteLockException {
		this.kind = kind;

		service = DatastoreServiceFactory.getDatastoreService();

		while (true) {
			Transaction transaction = service.beginTransaction();
			metadata = DatastoreStreamingIO.readMetadata(kind, service);

			long currentTime = System.currentTimeMillis();

			if (isWriteLocked(currentTime)) {
				transaction.rollback();
				throw new WriteLockException("'" + kind + "' is write-locked.");
			}

			metadata.version++;
			if (metadata.updateTime != metadata.createTime
					|| metadata.version != 0L) {
				metadata.updateTime = currentTime;
			}
			metadata.writeLocked = true;

			if (append) {
				page = metadata.page;
				pointer = metadata.pointer;
			} else {
				page = 0L;
				pointer = 0;
			}

			lastFlushPage = page;
			lastFlushPointer = pointer;

			try {
				DatastoreStreamingIO.writeMetadata(kind, service, metadata);
				transaction.commit();

				break;
			} catch (ConcurrentModificationException e) {
				if (transaction.isActive()) {
					transaction.rollback();
				}
			}
		}

		if (append) {
			try {
				Entity entity = service.get(KeyFactory.createKey(kind, Long
						.toHexString(page)));
				Blob data = (Blob) entity.getProperty(DATA__PROPERTY__DATA);
				if (data != null) {
					buffer = data.getBytes();
				} else {
					buffer = new byte[INITIAL_BUFFER_SIZE];
				}
			} catch (EntityNotFoundException e) {
				buffer = new byte[INITIAL_BUFFER_SIZE];
			}
		} else {
			buffer = new byte[INITIAL_BUFFER_SIZE];
		}
	}

	protected boolean isWriteLocked(long currentTime) {
		return metadata.writeLocked
				&& currentTime < metadata.updateTime + MAX_LOCK_TIME;
	}

	public DatastoreOutputStream(String kind) throws WriteLockException {
		this(kind, false);
	}

	@Override
	public void close() throws IOException {
		long currentTime = System.currentTimeMillis();

		if (!isWriteLocked(currentTime)) {
			return;
		}

		metadata.page = lastFlushPage;
		metadata.pointer = lastFlushPointer;
		metadata.updateTime = currentTime;
		metadata.writeLocked = false;

		DatastoreStreamingIO.writeMetadata(kind, service, metadata);
	}

	@Override
	public void flush() throws IOException {
		if (page == lastFlushPage && pointer == lastFlushPointer) {
			// if nothing is written
			return;
		}

		byte[] data;
		if (pointer == buffer.length) {
			data = buffer;
		} else {
			data = new byte[pointer];
			System.arraycopy(buffer, 0, data, 0, pointer);
		}

		Entity entity = new Entity(kind, Long.toHexString(page));
		entity.setProperty(DATA__PROPERTY__DATA, new Blob(data));
		entity.setProperty(DATA__PROPERTY__VERSION, metadata.version);
		service.put(entity);

		lastFlushPage = page;
		lastFlushPointer = pointer;

		if (pointer == buffer.length) {
			page++;
			pointer = 0;
		}
	}

	@Override
	public void write(int b) throws IOException {
		if (!isWriteLocked(System.currentTimeMillis())) {
			throw new IOException("This stream is closed or unlocked.");
		}

		if (pointer + 1 > buffer.length) {
			if (buffer.length < BUFFER_SIZE) {
				allocateBuffer();
			} else {
				flush();
			}
		}

		buffer[pointer] = (byte) b;
		pointer++;
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		if (!isWriteLocked(System.currentTimeMillis())) {
			throw new IOException("This stream is closed or unlocked.");
		}

		if ((off < 0) || (off > b.length) || (len < 0)
				|| ((off + len) > b.length) || ((off + len) < 0)) {
			throw new IndexOutOfBoundsException();
		} else if (len == 0) {
			return;
		}

		while (pointer + len > buffer.length) {
			if (buffer.length < BUFFER_SIZE) {
				allocateBuffer();
			} else {
				int vacancy = buffer.length - pointer;

				System.arraycopy(b, off, buffer, pointer, vacancy);
				pointer += vacancy;
				flush();

				len -= vacancy;
				off += vacancy;
			}
		}

		System.arraycopy(b, off, buffer, pointer, len);
		pointer += len;
	}

	protected void allocateBuffer() {
		byte[] newBuffer = new byte[Integers
				.min(buffer.length * 2, BUFFER_SIZE)];
		System.arraycopy(buffer, 0, newBuffer, 0, buffer.length);
		buffer = newBuffer;
	}
}
