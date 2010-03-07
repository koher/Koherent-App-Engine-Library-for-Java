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

import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

/**
 * A <code>DatastoreWriter</code> is a <code>Writer</code> to write character
 * streams to a Datastore used on Google App Engine for Java.
 * <code>DatastoreReader</code> and <code>DatastoreWriter</code> can be
 * alternatives of <code>FileReader</code> and <code>FileWriter</code>.
 * 
 * <p>
 * <code>FileWriter</code> is not available on Google App Engine for Java.
 * <code>DatastoreWriter</code> enables write character streams to a Datastore
 * in a similar way with <code>FileWriter</code>.
 * </p>
 * 
 * <p>
 * Characters written by <code>DatastoreWriter</code> can be read using a
 * <code>DatastoreWriter</code>.
 * </p>
 * 
 * @author koher
 * @version 0.1
 * @since 0.1
 * @see DatastoreReader
 * @see DatastoreOutputStream
 * @see java.io.FileWriter
 * @see java.io.Writer
 */
public class DatastoreWriter extends OutputStreamWriter {
	public DatastoreWriter(String kind, Charset cs) throws WriteLockException {
		this(kind, cs, false);
	}

	public DatastoreWriter(String kind, CharsetEncoder enc)
			throws WriteLockException {
		this(kind, enc, false);
	}

	public DatastoreWriter(String kind, String charsetName)
			throws UnsupportedEncodingException, WriteLockException {
		this(kind, charsetName, false);
	}

	public DatastoreWriter(String kind) throws WriteLockException {
		this(kind, false);
	}

	public DatastoreWriter(String kind, Charset cs, boolean append)
			throws WriteLockException {
		super(new DatastoreOutputStream(kind, append), cs);
	}

	public DatastoreWriter(String kind, CharsetEncoder enc, boolean append)
			throws WriteLockException {
		super(new DatastoreOutputStream(kind, append), enc);
	}

	public DatastoreWriter(String kind, String charsetName, boolean append)
			throws UnsupportedEncodingException, WriteLockException {
		super(new DatastoreOutputStream(kind, append), charsetName);
	}

	public DatastoreWriter(String kind, boolean append)
			throws WriteLockException {
		super(new DatastoreOutputStream(kind, append));
	}
}