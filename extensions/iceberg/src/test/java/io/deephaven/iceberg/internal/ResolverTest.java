//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.internal;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.iceberg.util.ColumnInstructions;
import io.deephaven.iceberg.util.FieldPath;
import io.deephaven.iceberg.util.Resolver;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

/**
 * testing mapping is tougher than testing inference because it needs to deal with type conversion logic
 */
class ResolverTest {

    private static final IntegerType IT = IntegerType.get();

    @Test
    void noSuchColumn() {
        try {
            Resolver.builder()
                    .schema(simpleSchema(IT))
                    .spec(PartitionSpec.unpartitioned())
                    .definition(simpleDefinition(Type.intType()))
                    .putColumnInstructions("F1", FieldPath.of(42))
                    .putColumnInstructions("F2", FieldPath.of(43))
                    .putColumnInstructions("F3", FieldPath.of(42))
                    .build();
        } catch (NoSuchColumnException e) {
            assertThat(e).hasMessageContaining("Unknown column names [F3], available column names are [F1, F2]");
        }
    }

    @Test
    void noSuchPath() {
        try {
            Resolver.builder()
                    .schema(simpleSchema(IT))
                    .spec(PartitionSpec.unpartitioned())
                    .definition(simpleDefinition(Type.intType()))
                    .putColumnInstructions("F1", FieldPath.of(42))
                    .putColumnInstructions("F2", FieldPath.of(44))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column F2");
            assertThat(e).cause().hasMessageContaining("id path not found, path=[44], context=[]");
        }
    }

    @Test
    void unmappedColumn() {
        // All DH columns must be mapped by default.
        try {
            Resolver.builder()
                    .schema(simpleSchema(IT))
                    .spec(PartitionSpec.unpartitioned())
                    .definition(simpleDefinition(Type.intType()))
                    .putColumnInstructions("F1", FieldPath.of(42))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Column `F2` is not mapped");
        }
        // But, there is support to allow them, which is necessary for cases where a Schema field has been deleted, but
        // we want to keep the column in DH
        Resolver.builder()
                .schema(simpleSchema(IT))
                .spec(PartitionSpec.unpartitioned())
                .definition(simpleDefinition(Type.intType()))
                .putColumnInstructions("F1", FieldPath.of(42))
                .allowUnmappedColumns(true)
                .build();
    }

    @Test
    void duplicateMapping() {
        // It's okay to map the same Iceberg field to different DH columns
        Resolver.builder()
                .schema(simpleSchema(IT))
                .spec(PartitionSpec.unpartitioned())
                .definition(TableDefinition.of(
                        ColumnDefinition.of("F1", Type.intType()),
                        ColumnDefinition.of("F2", Type.intType()),
                        ColumnDefinition.of("F3", Type.intType())))
                .putColumnInstructions("F1", FieldPath.of(42))
                .putColumnInstructions("F2", FieldPath.of(43))
                .putColumnInstructions("F3", FieldPath.of(43))
                .build();

    }

    @Test
    void unmappedIcebergField() {
        // It's okay to not map all the Iceberg fields
        Resolver.builder()
                .schema(new Schema(
                        NestedField.optional(42, "F1", IT),
                        NestedField.required(43, "F2", IT),
                        NestedField.required(44, "F3", IT)))
                .spec(PartitionSpec.unpartitioned())
                .definition(simpleDefinition(Type.intType()))
                .putColumnInstructions("F1", FieldPath.of(42))
                .putColumnInstructions("F2", FieldPath.of(43))
                .build();
    }

    @Test
    void invalidMappingType() {
        // TODO: we should try to be thorough in describing what we do and do not support
        try {
            Resolver.builder()
                    .schema(simpleSchema(IT))
                    .spec(PartitionSpec.unpartitioned())
                    .definition(simpleDefinition(Type.stringType()))
                    .putColumnInstructions("F1", FieldPath.of(42))
                    .putColumnInstructions("F2", FieldPath.of(43))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column F1");
            assertThat(e).cause().hasMessageContaining(
                    "Unable to map Iceberg type `int` to Deephaven type `io.deephaven.qst.type.StringType`");
        }
    }

    @Test
    void invalidFieldPath() {
        try {
            Resolver.builder()
                    .schema(simpleSchema(IT))
                    .spec(PartitionSpec.unpartitioned())
                    .definition(TableDefinition.of(
                            ColumnDefinition.of("F1", Type.intType()),
                            ColumnDefinition.of("F2", Type.intType())))
                    .putColumnInstructions("F1", FieldPath.of(42))
                    .putColumnInstructions("F2", FieldPath.of(44))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column F2");
            assertThat(e).cause().hasMessageContaining("id path not found, path=[44], context=[]");
        }
    }

    @Test
    void nestedStruct() {
        final Schema schema = new Schema(NestedField.optional(1, "S1", Types.StructType.of(
                NestedField.optional(2, "S2", Types.StructType.of(
                        NestedField.optional(3, "F1", IT),
                        NestedField.required(4, "F2", IT))))));
        Resolver.builder()
                .schema(schema)
                .spec(PartitionSpec.unpartitioned())
                .definition(TableDefinition.of(
                        ColumnDefinition.ofInt("S1_S2_F1"),
                        ColumnDefinition.ofInt("S1_S2_F2")))
                .putColumnInstructions("S1_S2_F1", FieldPath.of(1, 2, 3))
                .putColumnInstructions("S1_S2_F2", FieldPath.of(1, 2, 4))
                .build();
    }

    @Test
    void fieldPathToStruct() {
        final Schema schema = new Schema(NestedField.optional(1, "S1", Types.StructType.of(
                NestedField.optional(2, "S2", Types.StructType.of(
                        NestedField.optional(3, "F1", IT),
                        NestedField.required(4, "F2", IT))))));
        try {
            Resolver.builder()
                    .schema(schema)
                    .spec(PartitionSpec.unpartitioned())
                    .definition(TableDefinition.of(ColumnDefinition.ofInt("S1_S2")))
                    .putColumnInstructions("S1_S2", FieldPath.of(1, 2))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column S1_S2");
            assertThat(e).cause().hasMessageContaining(
                    "Only support mapping to primitive types, field=[2: S2: optional struct<3: F1: optional int, 4: F2: required int>]");
        }
    }

    @Test
    void partitionSpec() {
        Schema schema = simpleSchema(IT);
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("F1")
                .build();
        PartitionField partitionField = spec.fields().get(0);
        Resolver.builder()
                .schema(schema)
                .spec(spec)
                .definition(TableDefinition.of(
                        ColumnDefinition.of("F1", Type.intType()).withPartitioning(),
                        ColumnDefinition.of("F2", Type.intType())))
                .putColumnInstructions("F1", ColumnInstructions.partitionField(partitionField.fieldId()))
                .putColumnInstructions("F2", FieldPath.of(43))
                .build();
    }

    @Test
    void badPartitionFieldId() {
        Schema schema = simpleSchema(IT);
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("F1")
                .build();
        try {
            Resolver.builder()
                    .schema(schema)
                    .spec(spec)
                    .definition(TableDefinition.of(
                            ColumnDefinition.of("F1", Type.intType()).withPartitioning(),
                            ColumnDefinition.of("F2", Type.intType())))
                    .putColumnInstructions("F1", ColumnInstructions.partitionField(9999))
                    .putColumnInstructions("F2", FieldPath.of(43))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column F1");
            assertThat(e).cause().hasMessageContaining("Unable to find partition field id 9999");
        }
    }

    @Test
    void badPartitionType() {
        Schema schema = simpleSchema(IT);
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .bucket("F1", 99)
                .build();
        PartitionField partitionField = spec.fields().get(0);
        try {
            Resolver.builder()
                    .schema(schema)
                    .spec(spec)
                    .definition(TableDefinition.of(
                            ColumnDefinition.of("F1", Type.intType()).withPartitioning(),
                            ColumnDefinition.of("F2", Type.intType())))
                    .putColumnInstructions("F1", ColumnInstructions.partitionField(partitionField.fieldId()))
                    .putColumnInstructions("F2", FieldPath.of(43))
                    .build();
            failBecauseExceptionWasNotThrown(Resolver.MappingException.class);
        } catch (Resolver.MappingException e) {
            assertThat(e).hasMessageContaining("Unable to map Deephaven column F1");
            assertThat(e).cause().hasMessageContaining(
                    "Unable to map partitionField=[1000: F1_bucket: bucket[99](42)], only identity transform is supported");
        }
    }

    // @Test
    // void simpleSubset() {
    // final Mapping mapping = Mapping.builder()
    // .schema(simpleSchema(IT))
    // .definition(simpleDefinition(Type.intType()))
    // .putPath("F1", FieldPath.of(42))
    // .build();
    // }
    //
    // @Test
    // void name() {
    // Mapping.builder()
    // .schema(simpleSchema(IT))
    // .definition(TableDefinition.of(
    // ColumnDefinition.ofShort("F1"),
    // ColumnDefinition.ofShort("F2")))
    // .putPath("F1", FieldPath.of(42))
    // .putPath("F2", FieldPath.of(43))
    // .build();
    // }
    //
    // @Test
    // void ListType() {
    //
    // final Schema schema = new Schema(NestedField.optional(2, "L1", Types.ListType.ofOptional(1, IT)));
    //
    // Mapping.builder()
    // .schema(schema)
    // .definition(TableDefinition.of(ColumnDefinition.of("L1", Type.intType().arrayType())))
    // .putPath("L1", FieldPath.of(2))
    // .build();
    // }

    private static Schema simpleSchema(org.apache.iceberg.types.Type type) {
        return new Schema(
                NestedField.optional(42, "F1", type),
                NestedField.required(43, "F2", type));
    }

    private static TableDefinition simpleDefinition(Type<?> type) {
        return TableDefinition.of(
                ColumnDefinition.of("F1", type),
                ColumnDefinition.of("F2", type));
    }
}
