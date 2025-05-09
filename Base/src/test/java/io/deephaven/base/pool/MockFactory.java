//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.pool;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

import io.deephaven.base.testing.RecordingMockObject;
import io.deephaven.base.verify.Assert;

// --------------------------------------------------------------------
/**
 * Mock factory
 */
public class MockFactory<T> extends RecordingMockObject implements Supplier<T> {

    private final List<T> m_items = new LinkedList<T>();

    // ------------------------------------------------------------
    public void add(T t) {
        m_items.add(t);
    }

    // ------------------------------------------------------------
    @Override
    public T get() {
        recordActivity("get()");
        Assert.eqFalse(m_items.isEmpty(), "m_items.isEmpty()");
        return m_items.remove(0);
    }
}
