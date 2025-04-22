//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util;

import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import org.apache.iceberg.types.Types;

import java.util.Objects;

final class IcebergListCompat implements Type.Visitor<Boolean>, GenericType.Visitor<Boolean> {

    private final org.apache.iceberg.types.Type.PrimitiveType elementType;

    public IcebergListCompat(Types.ListType lt) {
        Objects.requireNonNull(lt);
        if (lt.elementType() == null) {
            throw new IllegalArgumentException("ListType must have an element type");
        }
        if (!lt.elementType().isPrimitiveType()) {
            throw new IllegalArgumentException("ListType must have a primitive element type, found " +
                    lt.elementType());
        }
        this.elementType = lt.elementType().asPrimitiveType();
    }

    @Override
    public Boolean visit(GenericType<?> genericType) {
        return genericType.walk((GenericType.Visitor<Boolean>) this);
    }

    @Override
    public Boolean visit(ArrayType<?, ?> arrayType) {
        final Type componentType = arrayType.componentType();
        if (!(componentType instanceof PrimitiveType<?>)) {
            return false;
        }
        // TODO Talk to Devin why we need to do this cast
        return (Boolean) componentType.walk(new IcebergPrimitiveCompat(elementType));
    }

    @Override
    public Boolean visit(BoxedType<?> boxedType) {
        return false;
    }

    @Override
    public Boolean visit(StringType stringType) {
        return false;
    }

    @Override
    public Boolean visit(InstantType instantType) {
        return false;
    }

    @Override
    public Boolean visit(CustomType<?> customType) {
        return false;
    }

    @Override
    public Boolean visit(PrimitiveType<?> primitiveType) {
        return false;
    }
}

