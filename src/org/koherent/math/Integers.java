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

package org.koherent.math;

public class Integers {
	private Integers() {
	}

	public static int min(int a, int b) {
		return a < b ? a : b;
	}

	public static int min(int... values) {
		int min = Integer.MAX_VALUE;

		for (int value : values) {
			if (value < min) {
				min = value;
			}
		}

		return min;
	}

	public static int max(int a, int b) {
		return a > b ? a : b;
	}

	public static int max(int... values) {
		int max = Integer.MIN_VALUE;

		for (int value : values) {
			if (value > max) {
				max = value;
			}
		}

		return max;
	}

	public static Integer min(Integer a, Integer b) {
		return a < b ? a : b;
	}

	public static Integer min(Integer... values) {
		if (values.length == 0) {
			return null;
		}

		Integer min = Integer.MAX_VALUE;

		for (Integer value : values) {
			if (value < min) {
				min = value;
			}
		}

		return min;
	}

	public static Integer max(Integer a, Integer b) {
		return a > b ? a : b;
	}

	public static Integer max(Integer... values) {
		if (values.length == 0) {
			return null;
		}

		Integer max = Integer.MIN_VALUE;

		for (Integer value : values) {
			if (value > max) {
				max = value;
			}
		}

		return max;
	}
}