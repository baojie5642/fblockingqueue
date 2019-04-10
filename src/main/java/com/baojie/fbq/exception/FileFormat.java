/*
 *  Copyright 2011 sunli [sunli1223@gmail.com][weibo.com@sunli1223]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.baojie.fbq.exception;

// 一些初始化的异常
public class FileFormat extends Exception {

	private static final long serialVersionUID = 6950322066714479555L;

	public FileFormat() {
		super();
	}

	public FileFormat(String message) {
		super(message);
	}

	public FileFormat(String message, Throwable cause) {
		super(message, cause);
	}

	public FileFormat(Throwable cause) {
		super(cause);
	}

}
