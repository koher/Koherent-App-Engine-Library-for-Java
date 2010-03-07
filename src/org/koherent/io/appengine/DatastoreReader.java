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

import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

/**
 * A <code>DatastoreReader</code> is a <code>Reader</code> to read character
 * streams from a Datastore used on Google App Engine for Java.
 * <code>DatastoreReader</code> and <code>DatastoreWriter</code> can be
 * alternatives of <code>FileReader</code> and <code>FileWriter</code>.
 * 
 * <p>
 * It is usual to use <code>DatastoreOutputStream</code> to write character
 * streams which can be read by <code>DatastoreReader</code>.
 * </p>
 * 
 * @author koher
 * @version 0.1
 * @since 0.1
 * @see DatastoreWriter
 * @see DatastoreInputStream
 * @see java.io.FileReader
 * @see java.io.Reader
 */
public class DatastoreReader extends InputStreamReader {
	public DatastoreReader(String kind, Charset cs) {
		super(new DatastoreInputStream(kind), cs);
	}

	public DatastoreReader(String kind, CharsetDecoder dec) {
		super(new DatastoreInputStream(kind), dec);
	}

	public DatastoreReader(String kind, String charsetName)
			throws UnsupportedEncodingException {
		super(new DatastoreInputStream(kind), charsetName);
	}

	public DatastoreReader(String kind) {
		super(new DatastoreInputStream(kind));
	}
}