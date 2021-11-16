package io.deephaven.engine.tuple.generated;

import gnu.trove.map.TIntObjectMap;
import io.deephaven.engine.tuple.CanonicalizableTuple;
import io.deephaven.engine.tuple.serialization.SerializationUtils;
import io.deephaven.engine.tuple.serialization.StreamingExternalizable;
import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.util.compare.LongComparisons;
import io.deephaven.util.compare.ObjectComparisons;
import org.jetbrains.annotations.NotNull;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * <p>3-Tuple (triple) key class composed of float, Object, and long elements.
 * <p>Generated by io.deephaven.replicators.TupleCodeGenerator.
 */
public class FloatObjectLongTuple implements Comparable<FloatObjectLongTuple>, Externalizable, StreamingExternalizable, CanonicalizableTuple<FloatObjectLongTuple> {

    private static final long serialVersionUID = 1L;

    private float element1;
    private Object element2;
    private long element3;

    private transient int cachedHashCode;

    public FloatObjectLongTuple(
            final float element1,
            final Object element2,
            final long element3
    ) {
        initialize(
                element1,
                element2,
                element3
        );
    }

    /** Public no-arg constructor for {@link Externalizable} support only. <em>Application code should not use this!</em> **/
    public FloatObjectLongTuple() {
    }

    private void initialize(
            final float element1,
            final Object element2,
            final long element3
    ) {
        this.element1 = element1;
        this.element2 = element2;
        this.element3 = element3;
        cachedHashCode = ((31 +
                Float.hashCode(element1)) * 31 +
                Objects.hashCode(element2)) * 31 +
                Long.hashCode(element3);
    }

    public final float getFirstElement() {
        return element1;
    }

    public final Object getSecondElement() {
        return element2;
    }

    public final long getThirdElement() {
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
        final FloatObjectLongTuple typedOther = (FloatObjectLongTuple) other;
        // @formatter:off
        return element1 == typedOther.element1 &&
               ObjectComparisons.eq(element2, typedOther.element2) &&
               element3 == typedOther.element3;
        // @formatter:on
    }

    @Override
    public final int compareTo(@NotNull final FloatObjectLongTuple other) {
        if (this == other) {
            return 0;
        }
        int comparison;
        // @formatter:off
        return 0 != (comparison = FloatComparisons.compare(element1, other.element1)) ? comparison :
               0 != (comparison = ObjectComparisons.compare(element2, other.element2)) ? comparison :
               LongComparisons.compare(element3, other.element3);
        // @formatter:on
    }

    @Override
    public void writeExternal(@NotNull final ObjectOutput out) throws IOException {
        out.writeFloat(element1);
        out.writeObject(element2);
        out.writeLong(element3);
    }

    @Override
    public void readExternal(@NotNull final ObjectInput in) throws IOException, ClassNotFoundException {
        initialize(
                in.readFloat(),
                in.readObject(),
                in.readLong()
        );
    }

    @Override
    public void writeExternalStreaming(@NotNull final ObjectOutput out, @NotNull final TIntObjectMap<SerializationUtils.Writer> cachedWriters) throws IOException {
        out.writeFloat(element1);
        StreamingExternalizable.writeObjectElement(out, cachedWriters, 1, element2);
        out.writeLong(element3);
    }

    @Override
    public void readExternalStreaming(@NotNull final ObjectInput in, @NotNull final TIntObjectMap<SerializationUtils.Reader> cachedReaders) throws Exception {
        initialize(
                in.readFloat(),
                StreamingExternalizable.readObjectElement(in, cachedReaders, 1),
                in.readLong()
        );
    }

    @Override
    public String toString() {
        return "FloatObjectLongTuple{" +
                element1 + ", " +
                element2 + ", " +
                element3 + '}';
    }

    @Override
    public FloatObjectLongTuple canonicalize(@NotNull final UnaryOperator<Object> canonicalizer) {
        final Object canonicalizedElement2 = canonicalizer.apply(element2);
        return canonicalizedElement2 == element2
                ? this : new FloatObjectLongTuple(element1, canonicalizedElement2, element3);
    }
}
