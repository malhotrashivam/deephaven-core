//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharChunkHasher and run "./gradlew replicateHashing" to regenerate
//
// @formatter:off
package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.HashCodes;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.util.type.TypeUtils;

import static io.deephaven.chunk.util.hashing.ChunkHasher.scrambleHash;

public class DoubleChunkHasher implements ChunkHasher {
    public static DoubleChunkHasher INSTANCE = new DoubleChunkHasher();

    private static void hashInitial(DoubleChunk<Values> values, WritableIntChunk<HashCodes> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            final double value = values.get(ii);
            destination.set(ii, hashInitialSingle(value));
        }
        destination.setSize(values.size());
    }

    private static void hashSecondary(DoubleChunk<Values> values, WritableIntChunk<HashCodes> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            destination.set(ii, hashUpdateSingle(destination.get(ii), values.get(ii)));
        }
        destination.setSize(values.size());
    }

    public static int hashInitialSingle(double value) {
        return scrambleHash(DoubleComparisons.hashCode(value));
    }

    public static int hashUpdateSingle(int existing, double newValue) {
        return existing * 31 + hashInitialSingle(newValue);
    }

    @Override
    public int hashInitial(Object value) {
        return hashInitialSingle(TypeUtils.unbox((Double) value));
    }

    @Override
    public int hashUpdate(int existing, Object value) {
        return hashUpdateSingle(existing, TypeUtils.unbox((Double) value));
    }

    @Override
    public void hashInitial(Chunk<Values> values, WritableIntChunk<HashCodes> destination) {
        hashInitial(values.asDoubleChunk(), destination);
    }

    @Override
    public void hashUpdate(Chunk<Values> values, WritableIntChunk<HashCodes> destination) {
        hashSecondary(values.asDoubleChunk(), destination);
    }
}
