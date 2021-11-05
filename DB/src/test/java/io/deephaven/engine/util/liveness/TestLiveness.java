package io.deephaven.engine.util.liveness;

import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.live.LiveTableMonitor;
import io.deephaven.engine.tables.utils.TableTools;
import io.deephaven.engine.v2.TstUtils;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for liveness code.
 */
public class TestLiveness extends TestCase {

    private boolean oldCheckLtm;
    private LivenessScope scope;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        LiveTableMonitor.DEFAULT.resetForUnitTests(false);
        oldCheckLtm = LiveTableMonitor.DEFAULT.setCheckTableOperations(false);
        scope = new LivenessScope();
        LivenessScopeStack.push(scope);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        LivenessScopeStack.pop(scope);
        scope.release();
        LiveTableMonitor.DEFAULT.setCheckTableOperations(oldCheckLtm);
        LiveTableMonitor.DEFAULT.resetForUnitTests(true);
    }

    @SuppressWarnings("JUnit4AnnotatedMethodInJUnit3TestCase")
    @Test
    public void testRecursion() {
        // noinspection AutoBoxing
        final Table input = TstUtils.testRefreshingTable(
                TstUtils.i(2, 3, 6, 7, 8, 10, 12, 15, 16).convertToTracking(),
                TstUtils.c("GroupedInts", 1, 1, 2, 2, 2, 3, 3, 3, 3));
        Table result = null;
        for (int ii = 0; ii < 4096; ++ii) {
            if (result == null) {
                result = input;
            } else {
                result = TableTools.merge(result, input).updateView("GroupedInts=GroupedInts+1")
                        .updateView("GroupedInts=GroupedInts-1");
            }
        }
    }
}
