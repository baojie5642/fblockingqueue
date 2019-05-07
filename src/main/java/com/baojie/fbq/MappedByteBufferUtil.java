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
package com.baojie.fbq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class MappedByteBufferUtil {

    private static final Logger log = LoggerFactory.getLogger(MappedByteBufferUtil.class);

    private static final String CLEAN_KEY = "cleaner";

    private MappedByteBufferUtil() {
        throw new IllegalArgumentException();
    }

    public static final void clean(final Object buffer) {
        if (null == buffer) {
            return;
        } else {
            doClean(buffer);
        }
    }

    private static final void doClean(final Object buffer) {
        AccessController.doPrivileged(new PrivilegedAction() {
            public Object run() {
                try {
                    Method clean = cleanMethod(buffer);
                    if (null == clean) {
                        unmap(buffer);
                    } else {
                        clean.setAccessible(true);
                        doInvoke(clean, buffer);
                    }
                } catch (Exception e) {
                    log.error(e.toString(), e);
                }
                return null;
            }
        });
    }

    private static final Method cleanMethod(final Object buffer) {
        try {
            return buffer.getClass().getMethod(CLEAN_KEY, new Class[0]);
        } catch (Throwable t) {
            log.error(t.toString(), t);
        }
        return null;
    }

    private static final void doInvoke(final Method clean, final Object buffer) {
        Cleaner cleaner = null;
        try {
            cleaner = (Cleaner) clean.invoke(buffer, new Object[0]);
            cleaner.clean();
        } catch (IllegalAccessException e) {
            log.error(e.toString(), e);
        } catch (InvocationTargetException e) {
            log.error(e.toString(), e);
        }
    }

    private static final void unmap(final Object buffer) {
        if (null == buffer) {
            return;
        }
        if (!(buffer instanceof MappedByteBuffer)) {
            return;
        } else {
            Cleaner cl = ((DirectBuffer) buffer).cleaner();
            if (cl != null) {
                cl.clean();
            }
        }
    }

}
