//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.iceberg.util.ColumnInstructions;
import io.deephaven.iceberg.util.Resolver;
import io.deephaven.parquet.table.location.ParquetColumnResolver;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.deephaven.iceberg.util.ColumnInstructions.schemaField;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Types.buildMessage;
import static org.apache.parquet.schema.Types.optional;
import static org.apache.parquet.schema.Types.optionalGroup;
import static org.assertj.core.api.Assertions.assertThat;

class ResolverFactorySimpleTest {

    private static final String DH1 = "dh_col1";
    private static final String DH2 = "dh_col2";

    private static final String PQ1 = "parquet_col1";
    private static final String PQ2 = "parquet_col2";

    private static final String I1 = "iceberg_col1";
    private static final String I2 = "iceberg_col2";

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofInt(DH1),
            ColumnDefinition.ofInt(DH2));

    private static final int I1_ID = 42;
    private static final int I2_ID = 43;

    private static final Schema SCHEMA = new Schema(
            NestedField.optional(I1_ID, I1, IntegerType.get()),
            NestedField.required(I2_ID, I2, IntegerType.get()));

    private static final Map<String, ColumnInstructions> COLUMN_INSTRUCTIONS = Map.of(
            DH1, schemaField(I1_ID),
            DH2, schemaField(I2_ID));

    private static void check(ParquetColumnResolver resolver, boolean hasColumn1, boolean hasColumn2) {
        if (hasColumn1) {
            assertThat(resolver.of(DH1)).hasValue(List.of(PQ1));
        } else {
            assertThat(resolver.of(DH1)).isEmpty();
        }
        if (hasColumn2) {
            assertThat(resolver.of(DH2)).hasValue(List.of(PQ2));
        } else {
            assertThat(resolver.of(DH2)).isEmpty();
        }
        nonDhNamesAlwaysEmpty(resolver);
    }

    private static void nonDhNamesAlwaysEmpty(ParquetColumnResolver resolver) {
        // We will never resolve for column names that aren't DH column names.
        assertThat(resolver.of(I1)).isEmpty();
        assertThat(resolver.of(I2)).isEmpty();
        assertThat(resolver.of(PQ1)).isEmpty();
        assertThat(resolver.of(PQ2)).isEmpty();
        assertThat(resolver.of("RandomColumnName")).isEmpty();
    }

    private static Resolver.Builder builder() {
        return Resolver.builder()
                .definition(TABLE_DEFINITION)
                .schema(SCHEMA)
                .spec(PartitionSpec.unpartitioned())
                .putAllColumnInstructions(COLUMN_INSTRUCTIONS);
    }

    @Test
    void normal() {
        final ResolverFactory f1 = factory(builder().build());
        // An illustration that the name mapping is about the parquet column names, not the Deephaven nor Iceberg names,
        // so this name mapping should have no effect
        final ResolverFactory f2 = factory(builder()
                .nameMapping(NameMapping.of(
                        MappedField.of(I1_ID, List.of(I1, DH1)),
                        MappedField.of(I2_ID, List.of(I2, DH2))))
                .build());
        for (final ResolverFactory factory : List.of(f1, f2)) {
            // Empty parquet schema, nothing can be resolved
            {
                final ParquetColumnResolver resolver = factory.of(buildMessage().named("root"));
                check(resolver, false, false);
            }

            // Parquet schema without field ids does not resolve with normal resolver
            {
                final ParquetColumnResolver resolver = factory.of(buildMessage()
                        .addFields(
                                optional(INT32).as(intType(32, true)).named(PQ1),
                                optional(INT32).as(intType(32, true)).named(PQ2))
                        .named("root"));
                check(resolver, false, false);
            }

            // Parquet schema with field ids, resolves based on the column instructions
            {
                final ParquetColumnResolver resolver = factory.of(buildMessage()
                        .addFields(
                                optional(INT32).id(I1_ID).as(intType(32, true)).named(PQ1),
                                optional(INT32).id(I2_ID).as(intType(32, true)).named(PQ2))
                        .named("root"));
                check(resolver, true, true);
            }

            // Parquet schema with duplicate field ids does not resolve the duplicated field (but does resolve correct
            // ones)
            {
                final ParquetColumnResolver resolver = factory.of(buildMessage()
                        .addFields(
                                optional(INT32).id(I1_ID).as(intType(32, true)).named(PQ1),
                                optional(INT32).id(I2_ID).as(intType(32, true)).named(PQ2),
                                optional(INT32).id(I2_ID).as(intType(32, true)).named("parquet_col3"))
                        .named("root"));
                check(resolver, true, false);
            }

            // Duplication detection is at the _current_ level; duplicate field ids at a different level will still
            // "work"
            {
                final ParquetColumnResolver resolver = factory.of(buildMessage()
                        .addFields(
                                optional(INT32).id(I1_ID).as(intType(32, true)).named(PQ1),
                                optional(INT32).id(I2_ID).as(intType(32, true)).named(PQ2),
                                optionalGroup()
                                        .addFields(
                                                optional(INT32).id(I1_ID).as(intType(32, true)).named("parquet_col3"),
                                                optional(INT32).id(I1_ID).as(intType(32, true)).named("parquet_col4"))
                                        .named("parquet_group_1"))
                        .named("root"));
                check(resolver, true, true);
            }
        }
    }

    @Test
    void nameMapping() {
        final ResolverFactory f1 = factory(builder()
                .nameMapping(NameMapping.of(
                        MappedField.of(I1_ID, PQ1),
                        MappedField.of(I2_ID, PQ2)))
                .build());
        // Unrelated mappings should have no effect
        final ResolverFactory f2 = factory(builder()
                .nameMapping(NameMapping.of(
                        MappedField.of(I1_ID, List.of(PQ1, I1, DH1)),
                        MappedField.of(I2_ID, List.of(PQ2, I2, DH2))))
                .build());
        for (ResolverFactory factory : List.of(f1, f2)) {
            // Empty parquet schema, nothing can be resolved
            {
                final ParquetColumnResolver resolver = factory.of(buildMessage().named("root"));
                check(resolver, false, false);
            }

            // Parquet schema without field ids, resolves based on fallback
            {
                final ParquetColumnResolver resolver = factory.of(buildMessage()
                        .addFields(
                                optional(INT32).as(intType(32, true)).named(PQ1),
                                optional(INT32).as(intType(32, true)).named(PQ2))
                        .named("root"));
                check(resolver, true, true);
            }

            // Parquet schema with duplicate names does not resolve the duplicated field (but does resolve correct ones)
            {
                final ParquetColumnResolver resolver = factory.of(buildMessage()
                        .addFields(
                                optional(INT32).as(intType(32, true)).named(PQ1),
                                optional(INT32).as(intType(32, true)).named(PQ2),
                                optional(INT32).as(intType(32, true)).named(PQ2))
                        .named("root"));
                check(resolver, true, false);
            }

            // Duplication detection is at the _current_ level; duplicate names at a different level will still "work"
            {
                final ParquetColumnResolver resolver = factory.of(buildMessage()
                        .addFields(
                                optional(INT32).as(intType(32, true)).named(PQ1),
                                optional(INT32).as(intType(32, true)).named(PQ2),
                                optionalGroup()
                                        .addFields(
                                                optional(INT32).as(intType(32, true)).named(PQ1),
                                                optional(INT32).as(intType(32, true)).named(PQ1))
                                        .named("parquet_group_1"))
                        .named("root"));
                check(resolver, true, true);
            }
        }
    }

    private static ResolverFactory factory(Resolver resolver) {
        return new ResolverFactory(resolver);
    }
}
