package io.deephaven.engine.tuple.generated;

import gnu.trove.map.TIntObjectMap;
import io.deephaven.engine.tuple.CanonicalizableTuple;
import io.deephaven.engine.tuple.serialization.SerializationUtils;
import io.deephaven.engine.tuple.serialization.StreamingExternalizable;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.util.compare.ObjectComparisons;
import org.jetbrains.annotations.NotNull;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * <p>3-Tuple (triple) key class composed of double, Object, and double elements.
 * <p>Generated by io.deephaven.replicators.TupleCodeGenerator.
 */
public class DoubleObjectDoubleTuple implements Comparable<DoubleObjectDoubleTuple>, Externalizable, StreamingExternalizable, CanonicalizableTuple<DoubleObjectDoubleTuple> {

    private static final long serialVersionUID = 1L;

    private double element1;
    private Object element2;
    private double element3;

    private transient int cachedHashCode;

    public DoubleObjectDoubleTuple(
            final double element1,
            final Object element2,
            final double element3
    ) {
        initialize(
                element1,
                element2,
                element3
        );
    }

    /** Public no-arg constructor for {@link Externalizable} support only. <em>Application code should not use this!</em> **/
    public DoubleObjectDoubleTuple() {
    }

    private void initialize(
            final double element1,
            final Object element2,
            final double element3
    ) {
        this.element1 = element1;
        this.element2 = element2;
        this.element3 = element3;
        cachedHashCode = ((31 +
                Double.hashCode(element1)) * 31 +
                Objects.hashCode(element2)) * 31 +
                Double.hashCode(element3);
    }

    public final double getFirstElement() {
        return element1;
    }

    public final Object getSecondElement() {
        return element2;
    }

    public final double getThirdElement() {
        return element3;
    }

    @Override
    public final int hashCode() {
        return cachedHashCode;
    }

    @Override
    public final boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        final DoubleObjectDoubleTuple typedOther = (DoubleObjectDoubleTuple) other;
        // @formatter:off
        return element1 == typedOther.element1 &&
               ObjectComparisons.eq(element2, typedOther.element2) &&
               element3 == typedOther.element3;
        // @formatter:on
    }

    @Override
    public final int compareTo(@NotNull final DoubleObjectDoubleTuple other) {
        if (this == other) {
            return 0;
        }
        int comparison;
        // @formatter:off
        return 0 != (comparison = DoubleComparisons.compare(element1, other.element1)) ? comparison :
               0 != (comparison = ObjectComparisons.compare(element2, other.element2)) ? comparison :
               DoubleComparisons.compare(element3, other.element3);
        // @formatter:on
    }

    @Override
    public void writeExternal(@NotNull final ObjectOutput out) throws IOException {
        out.writeDouble(element1);
        out.writeObject(element2);
        out.writeDouble(element3);
    }

    @Override
    public void readExternal(@NotNull final ObjectInput in) throws IOException, ClassNotFoundException {
        initialize(
                in.readDouble(),
                in.readObject(),
                in.readDouble()
        );
    }

    @Override
    public void writeExternalStreaming(@NotNull final ObjectOutput out, @NotNull final TIntObjectMap<SerializationUtils.Writer> cachedWriters) throws IOException {
        out.writeDouble(element1);
        StreamingExternalizable.writeObjectElement(out, cachedWriters, 1, element2);
        out.writeDouble(element3);
    }

    @Override
    public void readExternalStreaming(@NotNull final ObjectInput in, @NotNull final TIntObjectMap<SerializationUtils.Reader> cachedReaders) throws Exception {
        initialize(
                in.readDouble(),
                StreamingExternalizable.readObjectElement(in, cachedReaders, 1),
                in.readDouble()
        );
    }

    @Override
    public String toString() {
        return "DoubleObjectDoubleTuple{" +
                element1 + ", " +
                element2 + ", " +
                element3 + '}';
    }

    @Override
    public DoubleObjectDoubleTuple canonicalize(@NotNull final UnaryOperator<Object> canonicalizer) {
        final Object canonicalizedElement2 = canonicalizer.apply(element2);
        return canonicalizedElement2 == element2
                ? this : new DoubleObjectDoubleTuple(element1, canonicalizedElement2, element3);
    }
}
