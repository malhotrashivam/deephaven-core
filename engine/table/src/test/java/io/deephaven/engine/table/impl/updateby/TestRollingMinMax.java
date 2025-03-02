//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby;

import io.deephaven.api.ColumnName;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.PartitionedTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.CharGenerator;
import io.deephaven.engine.testutil.generator.SortedInstantGenerator;
import io.deephaven.engine.testutil.generator.TestDataGenerator;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.vector.ObjectVector;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateAppends;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.function.Basic.isNull;

@Category(OutOfBandTest.class)
public class TestRollingMinMax extends BaseUpdateByTest {
    /**
     * These are used in the static tests and leverage the Numeric class functions for verification. Additional tests
     * are performed on BigInteger/BigDecimal columns as well.
     */
    final String[] primitiveColumns = new String[] {
            "charCol",
            "byteCol",
            "shortCol",
            "intCol",
            "longCol",
            "floatCol",
            "doubleCol",
    };

    /**
     * These are used in the ticking table evaluations where we verify dynamic vs static tables.
     */
    final String[] columns = new String[] {
            "charCol",
            "byteCol",
            "shortCol",
            "intCol",
            "longCol",
            "floatCol",
            "doubleCol",
            "bigIntCol",
            "bigDecimalCol",
    };

    final int STATIC_TABLE_SIZE = 10_000;
    final int DYNAMIC_TABLE_SIZE = 1_000;
    final int DYNAMIC_UPDATE_SIZE = 100;
    final int DYNAMIC_UPDATE_STEPS = 20;

    private String[] getFormulas(boolean isMax, String[] columns) {
        return isMax
                ? Arrays.stream(columns).map(c -> c + "=max(" + c + ")").toArray(String[]::new)
                : Arrays.stream(columns).map(c -> c + "=min(" + c + ")").toArray(String[]::new);
    }

    // For verification, we will upcast some columns and use already-defined Numeric class functions.
    private String[] getUpcastingFormulas(String[] columns) {
        return Arrays.stream(columns)
                .map(c -> c.equals("charCol")
                        ? String.format("%s=(short)%s", c, c)
                        : null)
                .filter(Objects::nonNull)
                .toArray(String[]::new);
    }

    private String[] getDowncastingFormulas(String[] columns) {
        return Arrays.stream(columns)
                .map(c -> c.equals("charCol")
                        ? String.format("%s=(char)%s", c, c)
                        : null)
                .filter(Objects::nonNull)
                .toArray(String[]::new);
    }

    // region Object Helper functions

    @SuppressWarnings("unused") // Functions used via QueryLibrary
    @VisibleForTesting
    @TestUseOnly
    public static class Helpers {

        public static BigInteger minBigInt(ObjectVector<BigInteger> bigIntegerObjectVector) {
            if (bigIntegerObjectVector == null) {
                return null;
            }

            BigInteger min = BigInteger.valueOf(Long.MAX_VALUE);
            long count = 0;
            final long n = bigIntegerObjectVector.size();

            for (long i = 0; i < n; i++) {
                BigInteger val = bigIntegerObjectVector.get(i);
                if (!isNull(val)) {
                    if (val.compareTo(min) < 0) {
                        min = val;
                    }
                    count++;
                }
            }
            if (count == 0) {
                return null;
            }
            return min;
        }

        public static BigInteger maxBigInt(ObjectVector<BigInteger> bigIntegerObjectVector) {
            if (bigIntegerObjectVector == null) {
                return null;
            }

            BigInteger max = BigInteger.valueOf(Long.MIN_VALUE);
            long count = 0;
            final long n = bigIntegerObjectVector.size();

            for (long i = 0; i < n; i++) {
                BigInteger val = bigIntegerObjectVector.get(i);
                if (!isNull(val)) {
                    if (val.compareTo(max) > 0) {
                        max = val;
                    }
                    count++;
                }
            }
            if (count == 0) {
                return null;
            }
            return max;
        }

        public static BigDecimal minBigDec(ObjectVector<BigDecimal> bigDecimalObjectVector) {
            if (bigDecimalObjectVector == null) {
                return null;
            }

            BigDecimal min = new BigDecimal(Double.MAX_VALUE);
            long count = 0;
            final long n = bigDecimalObjectVector.size();

            for (long i = 0; i < n; i++) {
                BigDecimal val = bigDecimalObjectVector.get(i);
                if (!isNull(val)) {
                    if (val.compareTo(min) < 0) {
                        min = val;
                    }
                    count++;
                }
            }
            if (count == 0) {
                return null;
            }
            return min;
        }

        public static BigDecimal maxBigDec(ObjectVector<BigDecimal> bigDecimalObjectVector) {
            if (bigDecimalObjectVector == null) {
                return null;
            }

            BigDecimal max = new BigDecimal(Double.MIN_VALUE);
            long count = 0;
            final long n = bigDecimalObjectVector.size();

            for (long i = 0; i < n; i++) {
                BigDecimal val = bigDecimalObjectVector.get(i);
                if (!isNull(val)) {
                    if (val.compareTo(max) > 0) {
                        max = val;
                    }
                    count++;
                }
            }
            if (count == 0) {
                return null;
            }
            return max;
        }
    }

    private void doTestStaticZeroKeyBigNumbers(final QueryTable t, final int prevTicks, final int postTicks) {
        ExecutionContext.getContext().getQueryLibrary().importStatic(Helpers.class);

        // TEST MIN VALUES

        Table actual = t.updateBy(UpdateByOperation.RollingMin(prevTicks, postTicks, "bigIntCol", "bigDecimalCol"));
        Table expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "bigIntCol", "bigDecimalCol"))
                .update("bigIntCol=minBigInt(bigIntCol)", "bigDecimalCol=minBigDec(bigDecimalCol)");

        BigInteger[] biActual = ColumnVectors.ofObject(actual, "bigIntCol", BigInteger.class).toArray();
        BigInteger[] biExpected = ColumnVectors.ofObject(expected, "bigIntCol", BigInteger.class).toArray();

        Assert.eq(biActual.length, "array length", biExpected.length);
        for (int ii = 0; ii < biActual.length; ii++) {
            BigInteger actualVal = biActual[ii];
            BigInteger expectedVal = biExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }

        BigDecimal[] bdActual = ColumnVectors.ofObject(actual, "bigDecimalCol", BigDecimal.class).toArray();
        BigDecimal[] bdExpected = ColumnVectors.ofObject(expected, "bigDecimalCol", BigDecimal.class).toArray();

        Assert.eq(bdActual.length, "array length", bdExpected.length);
        for (int ii = 0; ii < bdActual.length; ii++) {
            BigDecimal actualVal = bdActual[ii];
            BigDecimal expectedVal = bdExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }

        // TEST MAX VALUES

        actual = t.updateBy(UpdateByOperation.RollingMax(prevTicks, postTicks, "bigIntCol", "bigDecimalCol"));
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "bigIntCol", "bigDecimalCol"))
                .update("bigIntCol=maxBigInt(bigIntCol)", "bigDecimalCol=maxBigDec(bigDecimalCol)");

        biActual = ColumnVectors.ofObject(actual, "bigIntCol", BigInteger.class).toArray();
        biExpected = ColumnVectors.ofObject(expected, "bigIntCol", BigInteger.class).toArray();

        Assert.eq(biActual.length, "array length", biExpected.length);
        for (int ii = 0; ii < biActual.length; ii++) {
            BigInteger actualVal = biActual[ii];
            BigInteger expectedVal = biExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }

        bdActual = ColumnVectors.ofObject(actual, "bigDecimalCol", BigDecimal.class).toArray();
        bdExpected = ColumnVectors.ofObject(expected, "bigDecimalCol", BigDecimal.class).toArray();

        Assert.eq(bdActual.length, "array length", bdExpected.length);
        for (int ii = 0; ii < bdActual.length; ii++) {
            BigDecimal actualVal = bdActual[ii];
            BigDecimal expectedVal = bdExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }
    }

    private void doTestStaticZeroKeyTimedBigNumbers(final QueryTable t, final Duration prevTime,
            final Duration postTime) {
        ExecutionContext.getContext().getQueryLibrary().importStatic(Helpers.class);

        // TEST MIN VALUES

        Table actual = t.updateBy(UpdateByOperation.RollingMin("ts", prevTime, postTime, "bigIntCol", "bigDecimalCol"));
        Table expected =
                t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "bigIntCol", "bigDecimalCol"))
                        .update("bigIntCol=minBigInt(bigIntCol)", "bigDecimalCol=minBigDec(bigDecimalCol)");

        BigInteger[] biActual = ColumnVectors.ofObject(actual, "bigIntCol", BigInteger.class).toArray();
        BigInteger[] biExpected = ColumnVectors.ofObject(expected, "bigIntCol", BigInteger.class).toArray();

        Assert.eq(biActual.length, "array length", biExpected.length);
        for (int ii = 0; ii < biActual.length; ii++) {
            BigInteger actualVal = biActual[ii];
            BigInteger expectedVal = biExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }

        BigDecimal[] bdActual = ColumnVectors.ofObject(actual, "bigDecimalCol", BigDecimal.class).toArray();
        BigDecimal[] bdExpected = ColumnVectors.ofObject(expected, "bigDecimalCol", BigDecimal.class).toArray();

        Assert.eq(bdActual.length, "array length", bdExpected.length);
        for (int ii = 0; ii < bdActual.length; ii++) {
            BigDecimal actualVal = bdActual[ii];
            BigDecimal expectedVal = bdExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }

        // TEST MAX VALUES

        actual = t.updateBy(UpdateByOperation.RollingMax("ts", prevTime, postTime, "bigIntCol", "bigDecimalCol"));
        expected = t.updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "bigIntCol", "bigDecimalCol"))
                .update("bigIntCol=maxBigInt(bigIntCol)", "bigDecimalCol=maxBigDec(bigDecimalCol)");

        biActual = ColumnVectors.ofObject(actual, "bigIntCol", BigInteger.class).toArray();
        biExpected = ColumnVectors.ofObject(expected, "bigIntCol", BigInteger.class).toArray();

        Assert.eq(biActual.length, "array length", biExpected.length);
        for (int ii = 0; ii < biActual.length; ii++) {
            BigInteger actualVal = biActual[ii];
            BigInteger expectedVal = biExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }

        bdActual = ColumnVectors.ofObject(actual, "bigDecimalCol", BigDecimal.class).toArray();
        bdExpected = ColumnVectors.ofObject(expected, "bigDecimalCol", BigDecimal.class).toArray();

        Assert.eq(bdActual.length, "array length", bdExpected.length);
        for (int ii = 0; ii < bdActual.length; ii++) {
            BigDecimal actualVal = bdActual[ii];
            BigDecimal expectedVal = bdExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }
    }

    private void doTestStaticBucketedBigNumbers(final QueryTable t, final int prevTicks, final int postTicks) {
        ExecutionContext.getContext().getQueryLibrary().importStatic(Helpers.class);

        // TEST MIN VALUES

        Table actual =
                t.updateBy(UpdateByOperation.RollingMin(prevTicks, postTicks, "bigIntCol", "bigDecimalCol"), "Sym");
        Table expected =
                t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "bigIntCol", "bigDecimalCol"), "Sym")
                        .update("bigIntCol=minBigInt(bigIntCol)", "bigDecimalCol=minBigDec(bigDecimalCol)");

        BigInteger[] biActual = ColumnVectors.ofObject(actual, "bigIntCol", BigInteger.class).toArray();
        BigInteger[] biExpected = ColumnVectors.ofObject(expected, "bigIntCol", BigInteger.class).toArray();

        Assert.eq(biActual.length, "array length", biExpected.length);
        for (int ii = 0; ii < biActual.length; ii++) {
            BigInteger actualVal = biActual[ii];
            BigInteger expectedVal = biExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }

        BigDecimal[] bdActual = ColumnVectors.ofObject(actual, "bigDecimalCol", BigDecimal.class).toArray();
        BigDecimal[] bdExpected = ColumnVectors.ofObject(expected, "bigDecimalCol", BigDecimal.class).toArray();

        Assert.eq(bdActual.length, "array length", bdExpected.length);
        for (int ii = 0; ii < bdActual.length; ii++) {
            BigDecimal actualVal = bdActual[ii];
            BigDecimal expectedVal = bdExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }

        // TEST MAX VALUES

        actual = t.updateBy(UpdateByOperation.RollingMax(prevTicks, postTicks, "bigIntCol", "bigDecimalCol"), "Sym");
        expected = t.updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, "bigIntCol", "bigDecimalCol"), "Sym")
                .update("bigIntCol=maxBigInt(bigIntCol)", "bigDecimalCol=maxBigDec(bigDecimalCol)");

        biActual = ColumnVectors.ofObject(actual, "bigIntCol", BigInteger.class).toArray();
        biExpected = ColumnVectors.ofObject(expected, "bigIntCol", BigInteger.class).toArray();

        Assert.eq(biActual.length, "array length", biExpected.length);
        for (int ii = 0; ii < biActual.length; ii++) {
            BigInteger actualVal = biActual[ii];
            BigInteger expectedVal = biExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }

        bdActual = ColumnVectors.ofObject(actual, "bigDecimalCol", BigDecimal.class).toArray();
        bdExpected = ColumnVectors.ofObject(expected, "bigDecimalCol", BigDecimal.class).toArray();

        Assert.eq(bdActual.length, "array length", bdExpected.length);
        for (int ii = 0; ii < bdActual.length; ii++) {
            BigDecimal actualVal = bdActual[ii];
            BigDecimal expectedVal = bdExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }
    }

    private void doTestStaticBucketedTimedBigNumbers(final QueryTable t, final Duration prevTime,
            final Duration postTime) {
        ExecutionContext.getContext().getQueryLibrary().importStatic(Helpers.class);

        // TEST MIN VALUES

        Table actual =
                t.updateBy(UpdateByOperation.RollingMin("ts", prevTime, postTime, "bigIntCol", "bigDecimalCol"), "Sym");
        Table expected = t
                .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "bigIntCol", "bigDecimalCol"), "Sym")
                .update("bigIntCol=minBigInt(bigIntCol)", "bigDecimalCol=minBigDec(bigDecimalCol)");

        BigInteger[] biActual = ColumnVectors.ofObject(actual, "bigIntCol", BigInteger.class).toArray();
        BigInteger[] biExpected = ColumnVectors.ofObject(expected, "bigIntCol", BigInteger.class).toArray();

        Assert.eq(biActual.length, "array length", biExpected.length);
        for (int ii = 0; ii < biActual.length; ii++) {
            BigInteger actualVal = biActual[ii];
            BigInteger expectedVal = biExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }

        BigDecimal[] bdActual = ColumnVectors.ofObject(actual, "bigDecimalCol", BigDecimal.class).toArray();
        BigDecimal[] bdExpected = ColumnVectors.ofObject(expected, "bigDecimalCol", BigDecimal.class).toArray();

        Assert.eq(bdActual.length, "array length", bdExpected.length);
        for (int ii = 0; ii < bdActual.length; ii++) {
            BigDecimal actualVal = bdActual[ii];
            BigDecimal expectedVal = bdExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }

        // TEST MAX VALUES

        actual = t.updateBy(UpdateByOperation.RollingMax("ts", prevTime, postTime, "bigIntCol", "bigDecimalCol"),
                "Sym");
        expected = t
                .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, "bigIntCol", "bigDecimalCol"), "Sym")
                .update("bigIntCol=maxBigInt(bigIntCol)", "bigDecimalCol=maxBigDec(bigDecimalCol)");

        biActual = ColumnVectors.ofObject(actual, "bigIntCol", BigInteger.class).toArray();
        biExpected = ColumnVectors.ofObject(expected, "bigIntCol", BigInteger.class).toArray();

        Assert.eq(biActual.length, "array length", biExpected.length);
        for (int ii = 0; ii < biActual.length; ii++) {
            BigInteger actualVal = biActual[ii];
            BigInteger expectedVal = biExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }

        bdActual = ColumnVectors.ofObject(actual, "bigDecimalCol", BigDecimal.class).toArray();
        bdExpected = ColumnVectors.ofObject(expected, "bigDecimalCol", BigDecimal.class).toArray();

        Assert.eq(bdActual.length, "array length", bdExpected.length);
        for (int ii = 0; ii < bdActual.length; ii++) {
            BigDecimal actualVal = bdActual[ii];
            BigDecimal expectedVal = bdExpected[ii];
            if (actualVal != null || expectedVal != null) {
                Assert.eqTrue(actualVal.compareTo(expectedVal) == 0, "values match");
            }
        }
    }
    // endregion Object Helper functions

    // region Static Zero Key Tests

    @Test
    public void testStaticZeroKeyAllNullVector() {
        final int prevTicks = 1;
        final int postTicks = 0;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyRev() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyFwdRevWindow() {
        final int prevTicks = 100;
        final int postTicks = 100;

        doTestStaticZeroKey(prevTicks, postTicks);
    }

    @Test
    public void testStaticZeroKeyTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ZERO;

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedFwd() {
        final Duration prevTime = Duration.ZERO;
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    @Test
    public void testStaticZeroKeyTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticZeroKeyTimed(prevTime, postTime);
    }

    private void doTestStaticZeroKey(final int prevTicks, final int postTicks) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)}).t;

        final Table actualMin = t.updateBy(UpdateByOperation.RollingMin(prevTicks, postTicks, primitiveColumns));
        final Table expectedMin = t.update(getUpcastingFormulas(primitiveColumns))
                .updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, primitiveColumns))
                .update(getFormulas(false, primitiveColumns))
                .update(getDowncastingFormulas(primitiveColumns));
        TstUtils.assertTableEquals(expectedMin, actualMin, TableDiff.DiffItems.DoublesExact);

        final Table actualMax = t.updateBy(UpdateByOperation.RollingMax(prevTicks, postTicks, primitiveColumns));
        final Table expectedMax = t.update(getUpcastingFormulas(primitiveColumns))
                .updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, primitiveColumns))
                .update(getFormulas(true, primitiveColumns))
                .update(getDowncastingFormulas(primitiveColumns));
        TstUtils.assertTableEquals(expectedMax, actualMax, TableDiff.DiffItems.DoublesExact);

        doTestStaticZeroKeyBigNumbers(t, prevTicks, postTicks);
    }

    private void doTestStaticZeroKeyTimed(final Duration prevTime, final Duration postTime) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, false, false, false, 0xFFFABBBC,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;

        final Table actualMin = t.updateBy(UpdateByOperation.RollingMin("ts", prevTime, postTime, primitiveColumns));
        final Table expectedMin = t.update(getUpcastingFormulas(primitiveColumns))
                .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns))
                .update(getFormulas(false, primitiveColumns))
                .update(getDowncastingFormulas(primitiveColumns));
        TstUtils.assertTableEquals(expectedMin, actualMin, TableDiff.DiffItems.DoublesExact);

        final Table actualMax = t.updateBy(UpdateByOperation.RollingMax("ts", prevTime, postTime, primitiveColumns));
        final Table expectedMax = t.update(getUpcastingFormulas(primitiveColumns))
                .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns))
                .update(getFormulas(true, primitiveColumns))
                .update(getDowncastingFormulas(primitiveColumns));
        TstUtils.assertTableEquals(expectedMax, actualMax, TableDiff.DiffItems.DoublesExact);

        doTestStaticZeroKeyTimedBigNumbers(t, prevTime, postTime);
    }

    // endregion

    // region Static Bucketed Tests

    @Test
    public void testStaticGroupedBucketed() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestStaticBucketed(true, prevTicks, postTicks);
    }

    @Test
    public void testStaticGroupedBucketedTimed() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestStaticBucketedTimed(true, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedRev() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestStaticBucketed(false, prevTicks, postTicks);
    }

    @Test
    public void testStaticBucketedRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;

        doTestStaticBucketed(false, prevTicks, postTicks);
    }

    @Test
    public void testStaticBucketedFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        doTestStaticBucketed(false, prevTicks, postTicks);
    }

    @Test
    public void testStaticBucketedFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        doTestStaticBucketed(false, prevTicks, postTicks);
    }

    @Test
    public void testStaticBucketedTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    @Test
    public void testStaticBucketedFwdRevWindowTimed() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(5);

        doTestStaticBucketedTimed(false, prevTime, postTime);
    }

    private void doTestStaticBucketed(boolean grouped, int prevTicks, int postTicks) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, grouped, false, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)}).t;

        final Table actualMin = t.updateBy(UpdateByOperation.RollingMin(prevTicks, postTicks, primitiveColumns));
        final Table expectedMin = t.update(getUpcastingFormulas(primitiveColumns))
                .updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, primitiveColumns))
                .update(getFormulas(false, primitiveColumns))
                .update(getDowncastingFormulas(primitiveColumns));
        TstUtils.assertTableEquals(expectedMin, actualMin, TableDiff.DiffItems.DoublesExact);

        final Table actualMax = t.updateBy(UpdateByOperation.RollingMax(prevTicks, postTicks, primitiveColumns), "Sym");
        final Table expectedMax = t.update(getUpcastingFormulas(primitiveColumns))
                .updateBy(UpdateByOperation.RollingGroup(prevTicks, postTicks, primitiveColumns), "Sym")
                .update(getFormulas(true, primitiveColumns))
                .update(getDowncastingFormulas(primitiveColumns));
        TstUtils.assertTableEquals(expectedMax, actualMax, TableDiff.DiffItems.DoublesExact);

        doTestStaticBucketedBigNumbers(t, prevTicks, postTicks);
    }

    private void doTestStaticBucketedTimed(boolean grouped, Duration prevTime, Duration postTime) {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, grouped, false, 0xFFFABBBC,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)}).t;

        final Table actualMin =
                t.updateBy(UpdateByOperation.RollingMin("ts", prevTime, postTime, primitiveColumns), "Sym");
        final Table expectedMin = t.update(getUpcastingFormulas(primitiveColumns))
                .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns), "Sym")
                .update(getFormulas(false, primitiveColumns))
                .update(getDowncastingFormulas(primitiveColumns));
        TstUtils.assertTableEquals(expectedMin, actualMin, TableDiff.DiffItems.DoublesExact);

        final Table actualMax =
                t.updateBy(UpdateByOperation.RollingMax("ts", prevTime, postTime, primitiveColumns), "Sym");
        final Table expectedMax = t.update(getUpcastingFormulas(primitiveColumns))
                .updateBy(UpdateByOperation.RollingGroup("ts", prevTime, postTime, primitiveColumns), "Sym")
                .update(getFormulas(true, primitiveColumns))
                .update(getDowncastingFormulas(primitiveColumns));
        TstUtils.assertTableEquals(expectedMax, actualMax, TableDiff.DiffItems.DoublesExact);

        doTestStaticBucketedTimedBigNumbers(t, prevTime, postTime);
    }

    // endregion

    // region Append Only Tests

    @Test
    public void testZeroKeyAppendOnlyRev() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;

        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyFwdRev() {
        final int prevTicks = 50;
        final int postTicks = 50;

        doTestAppendOnly(false, prevTicks, postTicks);
    }

    @Test
    public void testBucketedAppendOnlyRev() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestAppendOnly(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedAppendOnlyRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;

        doTestAppendOnly(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedAppendOnlyFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        doTestAppendOnly(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedAppendOnlyFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        doTestAppendOnly(true, prevTicks, postTicks);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);

        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testZeroKeyAppendOnlyTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(5);

        doTestAppendOnlyTimed(false, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);

        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedAppendOnlyTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(5);

        doTestAppendOnlyTimed(true, prevTime, postTime);
    }

    private void doTestAppendOnly(boolean bucketed, int prevTicks, int postTicks) {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;
        t.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        final EvalNugget[] nuggets = new EvalNugget[] {
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t.updateBy(UpdateByOperation.RollingMin(prevTicks, postTicks, columns), "Sym")
                                : t.updateBy(UpdateByOperation.RollingMin(prevTicks, postTicks, columns));
                    }
                },
                new EvalNugget() {
                    @Override
                    protected Table e() {
                        return bucketed
                                ? t.updateBy(UpdateByOperation.RollingMax(prevTicks, postTicks, columns), "Sym")
                                : t.updateBy(UpdateByOperation.RollingMax(prevTicks, postTicks, columns));
                    }
                }
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }
    }

    private void doTestAppendOnlyTimed(boolean bucketed, Duration prevTime, Duration postTime) {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;
        t.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);

        final EvalNugget[] nuggets = new EvalNugget[] {
                EvalNugget.from(() -> bucketed ? t.updateBy(
                        UpdateByOperation.RollingMin("ts", prevTime, postTime, columns), "Sym")
                        : t.updateBy(UpdateByOperation.RollingMin("ts", prevTime, postTime, columns))),
                EvalNugget.from(() -> bucketed ? t.updateBy(
                        UpdateByOperation.RollingMax("ts", prevTime, postTime, columns), "Sym")
                        : t.updateBy(UpdateByOperation.RollingMax("ts", prevTime, postTime, columns)))
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> generateAppends(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table", nuggets);
        }
    }

    // endregion Append Only Tests

    // region General Ticking Tests

    @Test
    public void testZeroKeyGeneralTickingRev() {
        final long prevTicks = 100;
        final long fwdTicks = 0;

        doTestTicking(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyGeneralTickingRevExclusive() {
        final long prevTicks = 100;
        final long fwdTicks = -50;

        doTestTicking(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyGeneralTickingFwd() {
        final long prevTicks = 0;
        final long fwdTicks = 100;

        doTestTicking(false, prevTicks, fwdTicks);
    }

    @Test
    public void testZeroKeyGeneralTickingFwdExclusive() {
        final long prevTicks = -50;
        final long fwdTicks = 100;

        doTestTicking(false, prevTicks, fwdTicks);
    }

    @Test
    public void testBucketedGeneralTickingRev() {
        final int prevTicks = 100;
        final int postTicks = 0;

        doTestTicking(false, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingRevExclusive() {
        final int prevTicks = 100;
        final int postTicks = -50;

        doTestTicking(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingFwd() {
        final int prevTicks = 0;
        final int postTicks = 100;

        doTestTicking(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingFwdExclusive() {
        final int prevTicks = -50;
        final int postTicks = 100;

        doTestTicking(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingFwdRev() {
        final int prevTicks = 50;
        final int postTicks = 50;

        doTestTicking(true, prevTicks, postTicks);
    }

    @Test
    public void testBucketedGeneralTickingTimedRev() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        doTestTickingTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedGeneralTickingTimedRevExclusive() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(-5);

        doTestTickingTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedGeneralTickingTimedFwd() {
        final Duration prevTime = Duration.ofMinutes(0);
        final Duration postTime = Duration.ofMinutes(10);

        doTestTickingTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedGeneralTickingTimedFwdExclusive() {
        final Duration prevTime = Duration.ofMinutes(-5);
        final Duration postTime = Duration.ofMinutes(10);

        doTestTickingTimed(true, prevTime, postTime);
    }

    @Test
    public void testBucketedGeneralTickingTimedFwdRev() {
        final Duration prevTime = Duration.ofMinutes(5);
        final Duration postTime = Duration.ofMinutes(5);

        doTestTickingTimed(true, prevTime, postTime);
    }

    private void doTestTicking(final boolean bucketed, final long prevTicks, final long fwdTicks) {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                EvalNugget.from(() -> bucketed ? t.updateBy(
                        UpdateByOperation.RollingMin(prevTicks, fwdTicks, columns), "Sym")
                        : t.updateBy(UpdateByOperation.RollingMin(prevTicks, fwdTicks, columns))),
                EvalNugget.from(() -> bucketed ? t.updateBy(
                        UpdateByOperation.RollingMax(prevTicks, fwdTicks, columns), "Sym")
                        : t.updateBy(UpdateByOperation.RollingMax(prevTicks, fwdTicks, columns)))
        };


        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    private void doTestTickingTimed(final boolean bucketed, final Duration prevTime, final Duration postTime) {
        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, bucketed, false, true, 0x31313131,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)});

        final QueryTable t = result.t;

        final EvalNugget[] nuggets = new EvalNugget[] {
                EvalNugget.from(() -> bucketed ? t.updateBy(
                        UpdateByOperation.RollingMin("ts", prevTime, postTime, columns), "Sym")
                        : t.updateBy(UpdateByOperation.RollingMin("ts", prevTime, postTime, columns))),
                EvalNugget.from(() -> bucketed ? t.updateBy(
                        UpdateByOperation.RollingMax("ts", prevTime, postTime, columns), "Sym")
                        : t.updateBy(UpdateByOperation.RollingMax("ts", prevTime, postTime, columns)))
        };


        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(
                    () -> GenerateTableUpdates.generateTableUpdates(DYNAMIC_UPDATE_SIZE, billy, t, result.infos));
            TstUtils.validate("Table - step " + ii, nuggets);
        }
    }

    @Test
    public void testBucketedGeneralTickingRevRedirected() {
        final int prevTicks = 100;
        final int postTicks = 0;

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)});
        final QueryTable t = result.t;

        final UpdateByControl control = UpdateByControl.builder().useRedirection(true).build();

        final EvalNugget[] nuggets = new EvalNugget[] {
                EvalNugget.from(() -> t.updateBy(control,
                        List.of(UpdateByOperation.RollingMin(prevTicks, postTicks, columns)),
                        ColumnName.from("Sym")))
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            try {
                simulateShiftAwareStep(DYNAMIC_UPDATE_SIZE, billy, t, result.infos, nuggets);
            } catch (Throwable ex) {
                System.out.println("Crapped out on step " + ii);
                throw ex;
            }
        }
    }

    @Test
    public void testBucketedGeneralTickingTimedRevRedirected() {
        final Duration prevTime = Duration.ofMinutes(10);
        final Duration postTime = Duration.ofMinutes(0);

        final CreateResult result = createTestTable(DYNAMIC_TABLE_SIZE, true, false, true, 0x31313131,
                new String[] {"ts", "charCol"}, new TestDataGenerator[] {new SortedInstantGenerator(
                        DateTimeUtils.parseInstant("2022-03-09T09:00:00.000 NY"),
                        DateTimeUtils.parseInstant("2022-03-09T16:30:00.000 NY")),
                        new CharGenerator('A', 'z', 0.1)});

        final QueryTable t = result.t;

        final UpdateByControl control = UpdateByControl.builder().useRedirection(true).build();

        final EvalNugget[] nuggets = new EvalNugget[] {
                EvalNugget.from(() -> t.updateBy(control,
                        List.of(UpdateByOperation.RollingMin("ts", prevTime, postTime, columns)),
                        ColumnName.from("Sym")))
        };

        final Random billy = new Random(0xB177B177);
        for (int ii = 0; ii < DYNAMIC_UPDATE_STEPS; ii++) {
            try {
                simulateShiftAwareStep(DYNAMIC_UPDATE_SIZE, billy, t, result.infos, nuggets);
            } catch (Throwable ex) {
                System.out.println("Crapped out on step " + ii);
                throw ex;
            }
        }
    }

    // endregion

    @Test
    public void testNegativeDatasets() {
        final Table t = TableTools.emptyTable(100)
                .update("double_x=-100.0",
                        "int_x=-100")
                .update("float_x=(float)double_x",
                        "long_x=(long)int_x",
                        "short_x=(short)int_x",
                        "byte_x=(byte)int_x",
                        "char_x=(char)int_x");

        final Table expected = t.updateBy(UpdateByOperation.RollingMax(3));

        // We can assert equality to the input table because the data values are constant.
        TstUtils.assertTableEquals(t, expected, TableDiff.DiffItems.DoublesExact);
    }

    @Test
    public void testPositiveDatasets() {
        final Table t = TableTools.emptyTable(100)
                .update("double_x=100.0",
                        "int_x=100")
                .update("float_x=(float)double_x",
                        "long_x=(long)int_x",
                        "short_x=(short)int_x",
                        "byte_x=(byte)int_x",
                        "char_x=(char)int_x");

        final Table expected = t.updateBy(UpdateByOperation.RollingMax(3));

        // We can assert equality to the input table because the data values are constant.
        TstUtils.assertTableEquals(t, expected, TableDiff.DiffItems.DoublesExact);
    }

    @Test
    public void testZeroDatasets() {
        final Table t = TableTools.emptyTable(100)
                .update("double_x=0.0",
                        "int_x=0")
                .update("float_x=(float)double_x",
                        "long_x=(long)int_x",
                        "short_x=(short)int_x",
                        "byte_x=(byte)int_x",
                        "char_x=(char)int_x");

        final Table expected = t.updateBy(UpdateByOperation.RollingMax(3));

        // We can assert equality to the input table because the data values are constant.
        TstUtils.assertTableEquals(t, expected, TableDiff.DiffItems.DoublesExact);
    }

    @Test
    public void testResultDataTypes() {
        final Instant baseInstant = DateTimeUtils.parseInstant("2023-01-01T00:00:00 NY");
        final ZoneId zone = ZoneId.of("America/Los_Angeles");

        QueryScope.addParam("baseInstant", baseInstant);
        QueryScope.addParam("baseLDT", LocalDateTime.ofInstant(baseInstant, zone));
        QueryScope.addParam("baseZDT", baseInstant.atZone(zone));

        final TableDefinition expectedDefinition = TableDefinition.of(
                ColumnDefinition.ofByte("byteCol"),
                ColumnDefinition.ofChar("charCol"),
                ColumnDefinition.ofShort("shortCol"),
                ColumnDefinition.ofInt("intCol"),
                ColumnDefinition.ofLong("longCol"),
                ColumnDefinition.ofFloat("floatCol"),
                ColumnDefinition.ofDouble("doubleCol"),
                ColumnDefinition.ofString("stringCol"),
                ColumnDefinition.fromGenericType("instantCol", Instant.class),
                ColumnDefinition.fromGenericType("ldtCol", LocalDateTime.class),
                ColumnDefinition.fromGenericType("zdtCol", ZonedDateTime.class));

        final String[] columnNames = expectedDefinition.getColumnNamesArray();

        final String[] updateStrings = new String[] {
                "byteCol=(byte)i",
                "charCol=(char)(i + 64)",
                "shortCol=(short)i",
                "intCol=i",
                "longCol=ii",
                "floatCol=(float)ii",
                "doubleCol=(double)ii",
                "stringCol=String.valueOf(i)",
                "instantCol=baseInstant.plusSeconds(i)",
                "ldtCol=baseLDT.plusSeconds(i)",
                "zdtCol=baseZDT.plusSeconds(i)",
        };

        // NOTE: boolean is not supported by RollingMinMaxSpec.applicableTo()
        final Table source = TableTools.emptyTable(20).update(updateStrings);

        // Verify all the source columns are the expected types.
        source.getDefinition().checkCompatibility(expectedDefinition);

        final Table expected = source.updateBy(UpdateByOperation.RollingMax(5, columnNames));

        // Verify all the result columns are the expected types.
        expected.getDefinition().checkCompatibility(expectedDefinition);
    }

    @Test
    public void testProxy() {
        final QueryTable t = createTestTable(STATIC_TABLE_SIZE, true, false, false, 0x31313131,
                new String[] {"charCol"},
                new TestDataGenerator[] {new CharGenerator('A', 'z', 0.1)}).t;

        final int prevTicks = 100;
        final int postTicks = 0;

        Table actual;
        Table expected;

        PartitionedTable pt = t.partitionBy("Sym");
        actual = pt.proxy()
                .updateBy(UpdateByOperation.RollingMin(prevTicks, postTicks))
                .target().merge().sort("Sym");
        expected = t.sort("Sym").updateBy(UpdateByOperation.RollingMin(prevTicks, postTicks), "Sym");
        TstUtils.assertTableEquals(expected, actual);

        actual = pt.proxy()
                .updateBy(UpdateByOperation.RollingMax(prevTicks, postTicks))
                .target().merge().sort("Sym");
        expected = t.sort("Sym").updateBy(UpdateByOperation.RollingMax(prevTicks, postTicks), "Sym");
        TstUtils.assertTableEquals(expected, actual);
    }
}
