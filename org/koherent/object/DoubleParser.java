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

package org.koherent.object;

public class DoubleParser implements Parser<Double> {
	private static DoubleParser instance = new DoubleParser();

	private DoubleParser() {
	}

	public static DoubleParser getInstance() {
		return instance;
	}

	@Override
	public Double parse(String string) {
		try {
			return Double.parseDouble(string);
		} catch (NumberFormatException e) {
			return null;
		}
	}
}
