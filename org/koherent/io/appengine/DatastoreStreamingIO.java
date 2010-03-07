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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;

public class DatastoreStreamingIO {
	public static final int BUFFER_SIZE = 1024 * 1024 - 512; // 1 megabyte - 512 bytes

	protected static final String META_DATA__KEY = "m";
	protected static final String META_DATA__PROPERTY__PAGE = "p";
	protected static final String META_DATA__PROPERTY__POINTER = "o";
	protected static final String META_DATA__PROPERTY__VERSION = "v";
	protected static final String META_DATA__PROPERTY__WRITE_LOCKED = "l";
	protected static final String META_DATA__PROPERTY__CREATE_TIME = "c";
	protected static final String META_DATA__PROPERTY__UPDATE_TIME = "u";

	protected static final String DATA__PROPERTY__DATA = "d";
	protected static final String DATA__PROPERTY__VERSION = "v";

	private DatastoreStreamingIO() {
	}

	protected static void writeMetadata(String kind, DatastoreService service,
			Metadata metadata) {
		Entity entity = new Entity(kind, META_DATA__KEY);
		entity.setProperty(META_DATA__PROPERTY__PAGE, metadata.page);
		entity.setProperty(META_DATA__PROPERTY__POINTER, metadata.pointer);
		entity.setProperty(META_DATA__PROPERTY__VERSION, metadata.version);
		entity.setProperty(META_DATA__PROPERTY__WRITE_LOCKED,
				metadata.writeLocked);
		entity.setProperty(META_DATA__PROPERTY__CREATE_TIME,
				metadata.createTime);
		entity.setProperty(META_DATA__PROPERTY__UPDATE_TIME,
				metadata.updateTime);

		service.put(entity);
	}

	protected static Metadata readMetadata(String kind, DatastoreService service) {
		try {
			Entity entity = service.get(KeyFactory.createKey(kind,
					META_DATA__KEY));

			long numberOfPages = (Long) entity
					.getProperty(META_DATA__PROPERTY__PAGE);
			long dataSizeOfFinalpage = (Long) entity
					.getProperty(META_DATA__PROPERTY__POINTER);
			long version = (Long) entity
					.getProperty(META_DATA__PROPERTY__VERSION);
			boolean writeLocked = (Boolean) entity
					.getProperty(META_DATA__PROPERTY__WRITE_LOCKED);
			long createdTime = (Long) entity
					.getProperty(META_DATA__PROPERTY__CREATE_TIME);
			long updateTime = (Long) entity
					.getProperty(META_DATA__PROPERTY__UPDATE_TIME);

			return new Metadata(numberOfPages, (int) dataSizeOfFinalpage,
					version, writeLocked, createdTime, updateTime);
		} catch (EntityNotFoundException e) {
			long time = System.currentTimeMillis();
			return new Metadata(0L, 0, 0L, false, time, time);
		}
	}

	protected static class Metadata {
		protected long page;
		protected int pointer;
		protected long version;
		protected boolean writeLocked;
		protected long createTime;
		protected long updateTime;

		public Metadata(long page, int pointer, long version,
				boolean writeLocked, long createdTime, long updateTime) {
			this.page = page;
			this.pointer = pointer;
			this.version = version;
			this.writeLocked = writeLocked;
			this.createTime = createdTime;
			this.updateTime = updateTime;
		}

		public long getPage() {
			return page;
		}

		public int getPointer() {
			return pointer;
		}

		public long getVersion() {
			return version;
		}

		public boolean isWriteLocked() {
			return writeLocked;
		}

		public long getCreateTime() {
			return createTime;
		}

		public long getUpdateTime() {
			return updateTime;
		}
	}
}
