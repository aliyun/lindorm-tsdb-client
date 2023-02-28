/*
 * Copyright Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aliyun.lindorm.tsdb.client.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;


/**
 * <p>Provides utilities for manipulating and examining
 * <code>Throwable</code> objects.</p>
 *
 * @version $Id: ExceptionUtils.java 1436770 2013-01-22 07:09:45Z ggregory $
 * @since 1.0
 */
public class ExceptionUtils {

    /**
     * <p>The names of methods commonly used to access a wrapped exception.</p>
     */
    // TODO: Remove in Lang 4.0
    private static final String[] CAUSE_METHOD_NAMES = {
            "getCause",
            "getNextException",
            "getTargetException",
            "getException",
            "getSourceException",
            "getRootCause",
            "getCausedByException",
            "getNested",
            "getLinkedException",
            "getNestedException",
            "getLinkedCause",
            "getThrowable",
    };

    /**
     * <p>
     * Public constructor allows an instance of <code>ExceptionUtils</code> to be created, although
     * that is not normally necessary.
     * </p>
     */
    public ExceptionUtils() {
        super();
    }

    //-----------------------------------------------------------------------

    /**
     * <p>Introspects the <code>Throwable</code> to obtain the cause.</p>
     *
     * <p>The method searches for methods with specific names that return a
     * <code>Throwable</code> object. This will pick up most wrapping exceptions,
     * including those from JDK 1.4.
     *
     * <p>The default list searched for are:</p>
     * <ul>
     * <li><code>getCause()</code></li>
     * <li><code>getNextException()</code></li>
     * <li><code>getTargetException()</code></li>
     * <li><code>getException()</code></li>
     * <li><code>getSourceException()</code></li>
     * <li><code>getRootCause()</code></li>
     * <li><code>getCausedByException()</code></li>
     * <li><code>getNested()</code></li>
     * </ul>
     *
     * <p>If none of the above is found, returns <code>null</code>.</p>
     *
     * @param throwable the throwable to introspect for a cause, may be null
     * @return the cause of the <code>Throwable</code>,
     * <code>null</code> if none found or null throwable input
     * @since 1.0
     * @deprecated This feature will be removed in Lang 4.0
     */
    @Deprecated
    public static Throwable getCause(final Throwable throwable) {
        return getCause(throwable, CAUSE_METHOD_NAMES);
    }

    /**
     * <p>Introspects the <code>Throwable</code> to obtain the cause.</p>
     *
     * <p>A <code>null</code> set of method names means use the default set.
     * A <code>null</code> in the set of method names will be ignored.</p>
     *
     * @param throwable   the throwable to introspect for a cause, may be null
     * @param methodNames the method names, null treated as default set
     * @return the cause of the <code>Throwable</code>,
     * <code>null</code> if none found or null throwable input
     * @since 1.0
     * @deprecated This feature will be removed in Lang 4.0
     */
    @Deprecated
    public static Throwable getCause(final Throwable throwable, String[] methodNames) {
        if (throwable == null) {
            return null;
        }

        if (methodNames == null) {
            methodNames = CAUSE_METHOD_NAMES;
        }

        for (final String methodName : methodNames) {
            if (methodName != null) {
                final Throwable cause = getCauseUsingMethodName(throwable, methodName);
                if (cause != null) {
                    return cause;
                }
            }
        }

        return null;
    }

    /**
     * <p>Introspects the <code>Throwable</code> to obtain the root cause.</p>
     *
     * <p>This method walks through the exception chain to the last element,
     * "root" of the tree, using {@link #getCause(Throwable)}, and returns that exception.</p>
     *
     * <p>From version 2.2, this method handles recursive cause structures
     * that might otherwise cause infinite loops. If the throwable parameter has a cause of itself,
     * then null will be returned. If the throwable parameter cause chain loops, the last element in
     * the chain before the loop is returned.</p>
     *
     * @param throwable the throwable to get the root cause for, may be null
     * @return the root cause of the <code>Throwable</code>,
     * <code>null</code> if none found or null throwable input
     */
    public static Throwable getRootCause(final Throwable throwable) {
        final List<Throwable> list = getThrowableList(throwable);
        return list.size() < 2 ? throwable : (Throwable) list.get(list.size() - 1);
    }

    /**
     * <p>Finds a <code>Throwable</code> by method name.</p>
     *
     * @param throwable  the exception to examine
     * @param methodName the name of the method to find and invoke
     * @return the wrapped exception, or <code>null</code> if not found
     */
    // TODO: Remove in Lang 4.0
    private static Throwable getCauseUsingMethodName(final Throwable throwable,
            final String methodName) {
        Method method = null;
        try {
            method = throwable.getClass().getMethod(methodName);
        } catch (final NoSuchMethodException ignored) { // NOPMD
            // exception ignored
        } catch (final SecurityException ignored) { // NOPMD
            // exception ignored
        }

        if (method != null && Throwable.class.isAssignableFrom(method.getReturnType())) {
            try {
                return (Throwable) method.invoke(throwable);
            } catch (final IllegalAccessException ignored) { // NOPMD
                // exception ignored
            } catch (final IllegalArgumentException ignored) { // NOPMD
                // exception ignored
            } catch (final InvocationTargetException ignored) { // NOPMD
                // exception ignored
            }
        }
        return null;
    }

    /**
     * <p>Returns the list of <code>Throwable</code> objects in the
     * exception chain.</p>
     *
     * <p>A throwable without cause will return a list containing
     * one element - the input throwable. A throwable with one cause will return a list containing
     * two elements. - the input throwable and the cause throwable. A <code>null</code> throwable
     * will return a list of size zero.</p>
     *
     * <p>This method handles recursive cause structures that might
     * otherwise cause infinite loops. The cause chain is processed until the end is reached, or
     * until the next item in the chain is already in the result set.</p>
     *
     * @param throwable the throwable to inspect, may be null
     * @return the list of throwables, never null
     * @since Commons Lang 2.2
     */
    public static List<Throwable> getThrowableList(Throwable throwable) {
        final List<Throwable> list = new ArrayList<Throwable>();
        while (throwable != null && list.contains(throwable) == false) {
            list.add(throwable);
            throwable = ExceptionUtils.getCause(throwable);
        }
        return list;
    }

}
