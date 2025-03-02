//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.pool;

import io.deephaven.base.MockFactory;
import io.deephaven.base.verify.RequirementFailure;
import junit.framework.TestCase;

import java.util.function.Function;

// --------------------------------------------------------------------
/**
 * Tests for {@link ThreadSafeLenientFixedSizePool}.
 */
public class TestThreadSafeLenientFixedSizePool extends TestCase {

    private MockFactory<Object> m_mockObjectFactory;
    private MockClearingProcedure<Object> m_mockClearingProcedure;

    // this size specially chosen to exactly fill the (smallest) ring buffer
    private static final Object[] OBJECTS = {
            "alpha", "bravo", "charlie", "delta",
            "echo", "foxtrot", "golf", "hotel",
            "igloo", "juliet", "kilo", "lima",
            "mike", "november"
    };

    // ----------------------------------------------------------------
    public void testThreadSafeLenientFixedSizePool() {
        m_mockObjectFactory = new MockFactory<Object>();
        m_mockClearingProcedure = new MockClearingProcedure<Object>();


        // create pool
        for (Object object : OBJECTS) {
            m_mockObjectFactory.add(object);
        }
        Pool<Object> pool = ThreadSafeLenientFixedSizePool.FACTORY.create(OBJECTS.length, m_mockObjectFactory,
                m_mockClearingProcedure);
        assertEquals("get()get()get()get()get()get()get()get()get()get()get()get()get()get()",
                m_mockObjectFactory.getActivityRecordAndReset());

        // take
        Object alphaObject = OBJECTS[0];
        assertSame(alphaObject, pool.take());
        checkNoOtherActivity();

        // give
        pool.give(alphaObject);
        assertEquals("accept(alpha)", m_mockClearingProcedure.getActivityRecordAndReset());
        checkNoOtherActivity();

        // give on full pool - should be dropped
        Object quebecObject = "quebec";
        pool.give(quebecObject);
        assertEquals("accept(quebec)", m_mockClearingProcedure.getActivityRecordAndReset());
        checkNoOtherActivity();

        // give null
        pool.give(null);
        checkNoOtherActivity();

        // take from pool
        for (int nIndex = 1; nIndex < OBJECTS.length; nIndex++) {
            assertSame(OBJECTS[nIndex], pool.take());
            checkNoOtherActivity();
        }

        // take from pool
        assertSame(alphaObject, pool.take());
        checkNoOtherActivity();

        // take on empty pool
        Object romeoObject = "romeo";
        m_mockObjectFactory.add(romeoObject);
        assertSame(romeoObject, pool.take());
        assertEquals("get()", m_mockObjectFactory.getActivityRecordAndReset());
        checkNoOtherActivity();

        // give
        for (Object object : OBJECTS) {
            pool.give(object);
            assertEquals("accept(" + object + ")", m_mockClearingProcedure.getActivityRecordAndReset());
            checkNoOtherActivity();
        }

        // take from pool
        assertSame(alphaObject, pool.take());
        checkNoOtherActivity();
    }

    // ----------------------------------------------------------------
    public void testThreadSafeLenientFixedSizePoolNoClearingProcedure() {
        m_mockObjectFactory = new MockFactory<Object>();
        m_mockClearingProcedure = new MockClearingProcedure<Object>();

        // create pool
        for (Object object : OBJECTS) {
            m_mockObjectFactory.add(object);
        }
        Pool<Object> pool = ThreadSafeLenientFixedSizePool.FACTORY.create(OBJECTS.length, m_mockObjectFactory, null);
        assertEquals("get()get()get()get()get()get()get()get()get()get()get()get()get()get()",
                m_mockObjectFactory.getActivityRecordAndReset());

        // take
        Object alphaObject = OBJECTS[0];
        assertSame(alphaObject, pool.take());
        checkNoOtherActivity();

        // give
        pool.give(alphaObject);
        checkNoOtherActivity();

        // give on full pool - should be dropped
        pool.give("quebec");
        checkNoOtherActivity();

        // give null
        pool.give(null);
        checkNoOtherActivity();
    }

    // ----------------------------------------------------------------
    public void testThreadSafeLenientFixedSizePoolNoFactory() {
        m_mockObjectFactory = new MockFactory<Object>();
        m_mockClearingProcedure = new MockClearingProcedure<Object>();

        // no factory

        RequirementFailure failure = null;
        try {
            new ThreadSafeLenientFixedSizePool<Object>(OBJECTS.length,
                    (Function<ThreadSafeLenientFixedSizePool<Object>, Object>) null, m_mockClearingProcedure);
        } catch (RequirementFailure requirementFailure) {
            failure = requirementFailure;
            // assertTrue(requirementFailure.isThisStackFrameCulprit(0));
        }
        assertNotNull(failure);
        failure = null;

        // too small
        try {
            new ThreadSafeLenientFixedSizePool<Object>(6, m_mockObjectFactory, m_mockClearingProcedure);
            fail("expected to throw");
        } catch (RequirementFailure expected) {
        }

        // minimum size
        for (Object object : OBJECTS) {
            m_mockObjectFactory.add(object);
        }
        new ThreadSafeLenientFixedSizePool<Object>(7, m_mockObjectFactory, null);
        assertEquals("get()get()get()get()get()get()get()", m_mockObjectFactory.getActivityRecordAndReset());

        // no factory
        try {
            ThreadSafeLenientFixedSizePool.FACTORY.create(OBJECTS.length, null, m_mockClearingProcedure);
            fail("expected to throw");
        } catch (RequirementFailure expected) {
        }

        // too small
        try {
            ThreadSafeLenientFixedSizePool.FACTORY.create(6, m_mockObjectFactory, m_mockClearingProcedure);
            fail("expected to throw");
        } catch (RequirementFailure expected) {
        }

        // minimum size
        for (Object object : OBJECTS) {
            m_mockObjectFactory.add(object);
        }
        ThreadSafeLenientFixedSizePool.FACTORY.create(7, m_mockObjectFactory, null);
        assertEquals("get()get()get()get()get()get()get()", m_mockObjectFactory.getActivityRecordAndReset());
    }

    // ----------------------------------------------------------------
    private void checkNoOtherActivity() {
        assertEquals("", m_mockObjectFactory.getActivityRecordAndReset());
        assertEquals("", m_mockClearingProcedure.getActivityRecordAndReset());
    }
}
