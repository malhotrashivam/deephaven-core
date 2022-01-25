package io.deephaven.kafka.publish;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.Table;
import io.deephaven.kafka.KafkaSchemaUtils;
import io.deephaven.kafka.ingest.GenericRecordUtil;
import io.deephaven.time.DateTime;
import io.deephaven.engine.util.string.StringUtils;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.type.TypeUtils;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class GenericRecordKeyOrValueSerializer implements KeyOrValueSerializer<GenericRecord> {
    /**
     * The table we are reading from.
     */
    private final Table source;

    /**
     * The Avro schema.
     */
    private final Schema schema;


    protected final List<GenericRecordFieldProcessor> fieldProcessors = new ArrayList<>();

    public GenericRecordKeyOrValueSerializer(final Table source,
            final Schema schema,
            final String[] columnNames,
            final String timestampFieldName) {
        this.source = source;
        if (schema.isUnion()) {
            throw new UnsupportedOperationException("Schemas defined as a union of records are not supported");
        }
        final Schema.Type type = schema.getType();
        if (type != Schema.Type.RECORD) {
            throw new IllegalArgumentException("The schema is not a toplevel record definition.");
        }
        final boolean haveTimestampField = !StringUtils.isNullOrEmpty(timestampFieldName);
        TimeUnit timestampFieldUnit = null;
        final List<Schema.Field> fields = schema.getFields();
        if (haveTimestampField) {
            timestampFieldUnit = checkTimestampFieldAndGetUnit(fields, timestampFieldName);
        }
        this.schema = schema;

        int i = 0;
        for (Schema.Field field : fields) {
            if (haveTimestampField && timestampFieldName.equals(field.name())) {
                continue;
            }
            if (columnNames == null) {
                makeFieldProcessor(field, null);
            } else {
                makeFieldProcessor(field, columnNames[i]);
            }
            ++i;
        }

        if (haveTimestampField) {
            fieldProcessors.add(new TimestampFieldProcessor(timestampFieldName, timestampFieldUnit));
        }
    }

    /** Check the timestamp field exists and is of the right logical type. */
    private static TimeUnit checkTimestampFieldAndGetUnit(final List<Schema.Field> fields,
            final String timestampFieldName) {
        // Find the field with the right name.
        Schema.Field timestampField = null;
        for (Schema.Field field : fields) {
            if (timestampFieldName.equals(field.name())) {
                timestampField = field;
                break;
            }
        }
        if (timestampField == null) {
            throw new IllegalArgumentException(
                    "Field of name timestampFieldName='" + timestampFieldName + "' not found.");
        }
        final Schema fieldSchema = timestampField.schema();
        final Schema.Type type = fieldSchema.getType();
        if (type != Schema.Type.LONG) {
            throw new IllegalArgumentException(
                    "Field of name timestampFieldName='" + timestampFieldName + "' has wrong type " + type);
        }
        final LogicalType logicalType = fieldSchema.getLogicalType();
        if (LogicalTypes.timestampMicros().equals(logicalType)) {
            return TimeUnit.MICROSECONDS;
        }
        if (LogicalTypes.timestampMillis().equals(logicalType)) {
            return TimeUnit.MILLISECONDS;
        }
        throw new IllegalStateException(
                "Field of name timestampFieldName='" + timestampFieldName +
                        "' has wrong logical type " + logicalType);
    }

    interface FieldContext extends SafeCloseable {
    }

    private abstract static class GenericRecordFieldProcessor {
        final String fieldName;

        public GenericRecordFieldProcessor(final String fieldName) {
            this.fieldName = fieldName;
        }

        abstract FieldContext makeContext(int size);

        abstract void processField(
                FieldContext fieldContext,
                WritableObjectChunk<GenericRecord, Values> avroChunk,
                RowSequence keys,
                boolean isRemoval);
    }

    private abstract static class GenericRecordFieldProcessorImpl<ChunkType extends Chunk<Values>>
            extends GenericRecordFieldProcessor {
        private final ColumnSource<?> chunkSource;

        public GenericRecordFieldProcessorImpl(final String fieldName, final ColumnSource<?> chunkSource) {
            super(fieldName);
            this.chunkSource = chunkSource;
        }

        class ContextImpl implements FieldContext {
            ChunkSource.GetContext getContext;

            ContextImpl(final int size) {
                getContext = chunkSource.makeGetContext(size);
            }

            @Override
            public void close() {
                getContext.close();
            }
        }

        @Override
        FieldContext makeContext(final int size) {
            return new ContextImpl(size);
        }

        abstract Object getFieldElement(int i, ChunkType inputChunk);

        @Override
        void processField(
                final FieldContext fieldContext,
                final WritableObjectChunk<GenericRecord, Values> avroChunk,
                final RowSequence keys,
                final boolean previous) {
            final ContextImpl contextImpl = (ContextImpl) fieldContext;
            final ChunkType inputChunk;
            if (previous) {
                inputChunk = (ChunkType) chunkSource.getPrevChunk(contextImpl.getContext, keys);
            } else {
                inputChunk = (ChunkType) chunkSource.getChunk(contextImpl.getContext, keys);
            }

            for (int ii = 0; ii < inputChunk.size(); ++ii) {
                avroChunk.get(ii).put(fieldName, getFieldElement(ii, inputChunk));
            }
        }
    }

    @FunctionalInterface
    private interface GetFieldElementFun<ChunkType extends Chunk<Values>> {
        Object get(final int ii, final ChunkType inputChunk);
    }

    private static <ChunkType extends Chunk<Values>> GenericRecordFieldProcessor makeGenericFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource,
            final GetFieldElementFun<ChunkType> fun) {
        return new GenericRecordFieldProcessorImpl<ChunkType>(
                fieldName, chunkSource) {
            @Override
            Object getFieldElement(
                    final int ii,
                    final ChunkType inputChunk) {
                return fun.get(ii, inputChunk);
            }
        };
    }

    private static GenericRecordFieldProcessor makeByteFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return makeGenericFieldProcessor(
                fieldName,
                chunkSource,
                (final int ii, final ByteChunk<Values> inputChunk) -> TypeUtils.box(inputChunk.get(ii)));
    }

    private static GenericRecordFieldProcessor makeCharFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return makeGenericFieldProcessor(
                fieldName,
                chunkSource,
                (final int ii, final CharChunk<Values> inputChunk) -> TypeUtils.box(inputChunk.get(ii)));
    }

    private static GenericRecordFieldProcessor makeShortFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return makeGenericFieldProcessor(
                fieldName,
                chunkSource,
                (final int ii, final ShortChunk<Values> inputChunk) -> TypeUtils.box(inputChunk.get(ii)));
    }

    private static GenericRecordFieldProcessor makeIntFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return makeGenericFieldProcessor(
                fieldName,
                chunkSource,
                (final int ii, final IntChunk<Values> inputChunk) -> TypeUtils.box(inputChunk.get(ii)));
    }

    private static GenericRecordFieldProcessor makeLongFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return makeGenericFieldProcessor(
                fieldName,
                chunkSource,
                (final int ii, final LongChunk<Values> inputChunk) -> TypeUtils.box(inputChunk.get(ii)));
    }

    private static GenericRecordFieldProcessor makeLongFieldProcessorWithInverseFactor(
            final String fieldName,
            final ColumnSource<?> chunkSource,
            final long denominator) {
        return makeGenericFieldProcessor(
                fieldName, chunkSource, (final int ii, final LongChunk<Values> inputChunk) -> {
                    final long raw = inputChunk.get(ii);
                    if (raw == QueryConstants.NULL_LONG) {
                        return null;
                    }
                    return raw / denominator;
                });
    }

    private static GenericRecordFieldProcessor makeFloatFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return makeGenericFieldProcessor(
                fieldName,
                chunkSource,
                (final int ii, final FloatChunk<Values> inputChunk) -> TypeUtils.box(inputChunk.get(ii)));
    }

    private static GenericRecordFieldProcessor makeDoubleFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return makeGenericFieldProcessor(
                fieldName,
                chunkSource,
                (final int ii, final DoubleChunk<Values> inputChunk) -> TypeUtils.box(inputChunk.get(ii)));
    }

    private static GenericRecordFieldProcessor makeObjectFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return makeGenericFieldProcessor(
                fieldName,
                chunkSource,
                (final int ii, final ObjectChunk<?, Values> inputChunk) -> inputChunk.get(ii));
    }

    private static GenericRecordFieldProcessor makeDateTimeToMillisFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return makeGenericFieldProcessor(
                fieldName,
                chunkSource,
                (final int ii, final ObjectChunk<?, Values> inputChunk) -> ((DateTime) inputChunk.get(ii)).getMillis());
    }

    private static GenericRecordFieldProcessor makeDateTimeToMicrosFieldProcessor(
            final String fieldName,
            final ColumnSource<?> chunkSource) {
        return makeGenericFieldProcessor(
                fieldName,
                chunkSource,
                (final int ii, final ObjectChunk<?, Values> inputChunk) -> ((DateTime) inputChunk.get(ii)).getMicros());
    }

    private static class TimestampFieldProcessor extends GenericRecordFieldProcessor {
        private final long fromNanosToUnitDenominator;

        public TimestampFieldProcessor(final String fieldName, final TimeUnit unit) {
            super(fieldName);
            switch (unit) {
                case MICROSECONDS:
                    fromNanosToUnitDenominator = 1000;
                    break;
                case MILLISECONDS:
                    fromNanosToUnitDenominator = 1000 * 1000;
                    break;
                default:
                    throw new IllegalStateException("Unit not supported: " + unit);
            }
        }

        @Override
        FieldContext makeContext(final int size) {
            return null;
        }

        @Override
        public void processField(
                final FieldContext fieldContext,
                final WritableObjectChunk<GenericRecord, Values> avroChunk,
                final RowSequence keys,
                final boolean isRemoval) {
            final long nanos = DateTime.now().getNanos();
            for (int ii = 0; ii < avroChunk.size(); ++ii) {
                avroChunk.get(ii).put(fieldName, nanos / fromNanosToUnitDenominator);
            }
        }
    }

    private GenericRecordFieldProcessor getLongProcessor(
            final Schema.Field field,
            final String fieldName,
            final Class<?> columnType,
            final ColumnSource<?> src) {
        final Schema fieldSchema = field.schema();
        if (columnType == DateTime.class && fieldSchema.getType() == Schema.Type.LONG) {
            final LogicalType logicalType = fieldSchema.getLogicalType();
            if (LogicalTypes.timestampMicros().equals(logicalType)) {
                return makeLongFieldProcessorWithInverseFactor(fieldName, src, 1000);
            }
            if (LogicalTypes.timestampMillis().equals(logicalType)) {
                return makeLongFieldProcessorWithInverseFactor(fieldName, src, 1000 * 1000);
            }
        }
        return makeLongFieldProcessor(fieldName, src);
    }

    final String getLogicalType(final String fieldName, final Schema.Field field) {
        final Schema effectiveSchema = KafkaSchemaUtils.getEffectiveSchema(fieldName, field.schema());
        return effectiveSchema.getProp("logicalType");
    }

    /**
     * Create a field processor that translates a given column from its Deephaven row number to output of the intended
     * type.
     *
     * @param field The field in the schema.
     * @param columnNameIn The Deephaven column to be translated into publishable format
     */
    private void makeFieldProcessor(final Schema.Field field, final String columnNameIn) {
        final String fieldName = field.name();
        final String columnName = (columnNameIn == null) ? fieldName : columnNameIn;
        // getColumnSource should throw a ColumnNotFoundException if it can't find the column,
        // which will blow us up here.
        final ColumnSource<?> src = source.getColumnSource(columnName);
        final Class<?> type = src.getType();
        final GenericRecordFieldProcessor proc;
        if (type == char.class) {
            proc = makeCharFieldProcessor(fieldName, src);
        } else if (type == byte.class) {
            proc = makeByteFieldProcessor(fieldName, src);
        } else if (type == short.class) {
            proc = makeShortFieldProcessor(fieldName, src);
        } else if (type == int.class) {
            proc = makeIntFieldProcessor(fieldName, src);
        } else if (type == long.class) {
            proc = getLongProcessor(field, fieldName, source.getColumn(columnName).getType(), src);
        } else if (type == float.class) {
            proc = makeFloatFieldProcessor(fieldName, src);
        } else if (type == double.class) {
            proc = makeDoubleFieldProcessor(fieldName, src);
        } else if (type == DateTime.class) {
            final String logicalType = getLogicalType(fieldName, field);
            if (logicalType == null) {
                throw new IllegalArgumentException(
                        "field " + fieldName + " for column " + columnName + " has no logical type.");
            }
            if (logicalType.equals("timestamp-millis")) {
                proc = makeDateTimeToMillisFieldProcessor(fieldName, src);
            } else if (logicalType.equals("timestamp-micros")) {
                proc = makeDateTimeToMicrosFieldProcessor(fieldName, src);
            } else {
                throw new IllegalArgumentException("field " + fieldName + " for column " + columnName
                        + " has unrecognized logical type " + logicalType);
            }
        } else {
            proc = makeObjectFieldProcessor(fieldName, src);
        }
        fieldProcessors.add(proc);
    }

    /**
     * Process the given update RowSequence and returns a list of JSON strings, reach representing one row of data.
     *
     * @param toProcess A RowSequence indicating which rows were involved
     * @param previous True if this should be performed using the 'previous' data instead of current, as for removals.
     * @return A List of Strings containing all of the parsed update statements
     */
    @Override
    public ObjectChunk<GenericRecord, Values> handleChunk(Context context, RowSequence toProcess,
            boolean previous) {
        final AvroContext avroContext = (AvroContext) context;

        avroContext.avroChunk.setSize(toProcess.intSize());
        for (int position = 0; position < toProcess.intSize(); ++position) {
            avroContext.avroChunk.set(position, new GenericData.Record(schema));
        }

        for (int ii = 0; ii < fieldProcessors.size(); ++ii) {
            fieldProcessors.get(ii).processField(
                    avroContext.fieldContexts[ii],
                    avroContext.avroChunk,
                    toProcess,
                    previous);
        }

        return avroContext.avroChunk;
    }

    @Override
    public Context makeContext(int size) {
        return new AvroContext(size);
    }

    private final class AvroContext implements Context {

        private final WritableObjectChunk<GenericRecord, Values> avroChunk;
        private final FieldContext[] fieldContexts;

        public AvroContext(int size) {
            this.avroChunk = WritableObjectChunk.makeWritableChunk(size);
            this.fieldContexts = new FieldContext[fieldProcessors.size()];
            for (int ii = 0; ii < fieldProcessors.size(); ++ii) {
                fieldContexts[ii] = fieldProcessors.get(ii).makeContext(size);
            }
        }

        @Override
        public void close() {
            avroChunk.close();
            SafeCloseable.closeArray(fieldContexts);
        }
    }
}
