//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.util.NameValidator;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.immutables.value.Value;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Value.Immutable
@BuildableStyle
public abstract class InferenceInstructions {

    public static Builder builder() {
        return ImmutableInferenceInstructions.builder();
    }

    public static InferenceInstructions of(Schema schema, PartitionSpec partitionSpec) {
        return builder().schema(schema).spec(partitionSpec).build();
    }

    /**
     * The schema to use for inference.
     */
    public abstract Schema schema();

    /**
     * The partition spec to use for inference.
     */
    public abstract PartitionSpec spec();

    /**
     * The namer factory. Defaults to {@link Namer.Factory#fieldName()}.
     */
    @Value.Default
    public Namer.Factory namerFactory() {
        return Namer.Factory.fieldName();
    }

    /**
     * If inference should fail if any of the Iceberg fields fail to map to Deephaven columns. A {@link #skip() skipped}
     * field will not throw an exception. By default, is {@code false}.
     */
    @Value.Default
    public boolean failOnUnsupportedTypes() {
        return false;
    }

    /**
     * The set of field paths to skip during inference.
     */
    public abstract Set<FieldPath> skip();

    /**
     * The Deephaven column namer.
     */
    public interface Namer {

        interface Factory {

            /**
             * The field name {@link Namer} constructs a Deephaven column name by joining together the
             * {@link Types.NestedField#name() field names} with an underscore ({@code _}) and calling
             * {@link NameValidator#legalizeColumnName(String, Set)} with de-duplication logic.
             */
            static Factory fieldName() {
                return FieldNameNamer.FactoryImpl.FIELD_NAME_NAMER;
            }

            /**
             * The field name {@link Namer} constructs a Deephaven column name of the form {@value FieldIdNamer#FIELD_ID} with the last {@link Types.NestedField#fieldId() field-id} in the path appended.
             */
            static Factory fieldId() {
                return FieldIdNamer.FIELD_ID_NAMER;
            }

            /**
             * Creates a new namer instance.
             */
            Namer create();
        }

        /**
         * Called for each field path that Deephaven is inferring. Implementations must ensure they return a valid,
         * unique column name.
         *
         * @param path the nested field path
         * @param type the type
         * @return the Deephaven column name
         */
        String of(Collection<? extends Types.NestedField> path, Type<?> type);
    }

    public interface Builder {
        Builder schema(Schema schema);

        Builder spec(PartitionSpec spec);

        Builder failOnUnsupportedTypes(boolean failOnUnsupportedTypes);

        Builder namerFactory(Namer.Factory namerFactory);

        Builder addSkip(FieldPath element);

        Builder addSkip(FieldPath... elements);

        Builder addAllSkip(Iterable<? extends FieldPath> elements);

        InferenceInstructions build();
    }

    @Value.Check
    final void checkSpecSchema() {
        if (spec() == PartitionSpec.unpartitioned()) {
            return;
        }
        if (!schema().sameSchema(spec().schema())) {
            throw new IllegalArgumentException("schema and spec schema are not the same");
        }
    }

    private static final class FieldNameNamer implements Namer {

        private enum FactoryImpl implements Factory {
            FIELD_NAME_NAMER;

            @Override
            public Namer create() {
                return new FieldNameNamer();
            }
        }

        private final Set<String> usedNames = new HashSet<>();

        @Override
        public String of(Collection<? extends Types.NestedField> path, Type<?> type) {
            final String joinedNames = path.stream().map(Types.NestedField::name).collect(Collectors.joining("_"));
            final String columnName = NameValidator.legalizeColumnName(joinedNames, usedNames);
            usedNames.add(columnName);
            return columnName;
        }
    }

    private enum FieldIdNamer implements Namer.Factory, Namer {
        FIELD_ID_NAMER;

        private static final String FIELD_ID = "FieldId_";

        @Override
        public Namer create() {
            return this;
        }

        @Override
        public String of(Collection<? extends Types.NestedField> path, Type<?> type) {
            Types.NestedField lastField = null;
            for (Types.NestedField nestedField : path) {
                lastField = nestedField;
            }
            if (lastField == null) {
                throw new IllegalStateException();
            }
            return FIELD_ID + lastField.fieldId();
        }
    }
}
