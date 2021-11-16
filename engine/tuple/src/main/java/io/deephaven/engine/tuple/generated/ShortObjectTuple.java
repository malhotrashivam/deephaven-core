package io.deephaven.engine.tuple.generated;

import gnu.trove.map.TIntObjectMap;
import io.deephaven.engine.tuple.CanonicalizableTuple;
import io.deephaven.engine.tuple.serialization.SerializationUtils;
import io.deephaven.engine.tuple.serialization.StreamingExternalizable;
import io.deephaven.util.compare.ObjectComparisons;
import io.deephaven.util.compare.ShortComparisons;
import org.jetbrains.annotations.NotNull;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * <p>2-Tuple (double) key class composed of short and Object elements.
 * <p>Generated by io.deephaven.replicators.TupleCodeGenerator.
 */
public class ShortObjectTuple implements Comparable<ShortObjectTuple>, Externalizable, StreamingExternalizable, CanonicalizableTuple<ShortObjectTuple> {

    private static final long serialVersionUID = 1L;

    private short element1;
    private Object element2;

    private transient int cachedHashCode;

    public ShortObjectTuple(
            final short element1,
            final Object element2
    ) {
        initialize(
                element1,
                element2
        );
    }

    /** Public no-arg constructor for {@link Externalizable} support only. <em>Application code should not use this!</em> **/
    public ShortObjectTuple() {
    }

    private void initialize(
            final short element1,
            final Object element2
    ) {
        this.element1 = element1;
        this.element2 = element2;
        cachedHashCode = (31 +
                Short.hashCode(element1)) * 31 +
                Objects.hashCode(element2);
    }

    public final short getFirstElement() {
        return element1;
    }

    public final Object getSecondElement() {
        return element2;
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
        final ShortObjectTuple typedOther = (ShortObjectTuple) other;
        // @formatter:off
        return element1 == typedOther.element1 &&
               ObjectComparisons.eq(element2, typedOther.element2);
        // @formatter:on
    }

    @Override
    public final int compareTo(@NotNull final ShortObjectTuple other) {
        if (this == other) {
            return 0;
        }
        int comparison;
        // @formatter:off
        return 0 != (comparison = ShortComparisons.compare(element1, other.element1)) ? comparison :
               ObjectComparisons.compare(element2, other.element2);
        // @formatter:on
    }

    @Override
    public void writeExternal(@NotNull final ObjectOutput out) throws IOException {
        out.writeShort(element1);
        out.writeObject(element2);
    }

    @Override
    public void readExternal(@NotNull final ObjectInput in) throws IOException, ClassNotFoundException {
        initialize(
                in.readShort(),
                in.readObject()
        );
    }

    @Override
    public void writeExternalStreaming(@NotNull final ObjectOutput out, @NotNull final TIntObjectMap<SerializationUtils.Writer> cachedWriters) throws IOException {
        out.writeShort(element1);
        StreamingExternalizable.writeObjectElement(out, cachedWriters, 1, element2);
    }

    @Override
    public void readExternalStreaming(@NotNull final ObjectInput in, @NotNull final TIntObjectMap<SerializationUtils.Reader> cachedReaders) throws Exception {
        initialize(
                in.readShort(),
                StreamingExternalizable.readObjectElement(in, cachedReaders, 1)
        );
    }

    @Override
    public String toString() {
        return "ShortObjectTuple{" +
                element1 + ", " +
                element2 + '}';
    }

    @Override
    public ShortObjectTuple canonicalize(@NotNull final UnaryOperator<Object> canonicalizer) {
        final Object canonicalizedElement2 = canonicalizer.apply(element2);
        return canonicalizedElement2 == element2
                ? this : new ShortObjectTuple(element1, canonicalizedElement2);
    }
}
