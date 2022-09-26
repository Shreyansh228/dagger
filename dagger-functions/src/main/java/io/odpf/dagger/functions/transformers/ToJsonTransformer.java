package io.odpf.dagger.functions.transformers;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.common.core.Transformer;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * Converts to Row object json string and added to the inputRow as last column.
 * This is required to convert the input row to jsonString.
 */
public class ToJsonTransformer extends RichMapFunction<Row, Row> implements Serializable, Transformer {

    private static final String KEY_COLUMN_NAME = "jsonColumnName";
    private final String keyColumn;
    private final String[] columnNames;

    /**
     * Instantiates a new Feature with type transformer.
     *
     * @param transformationArguments the transformation arguments
     * @param columnNames             the column names
     * @param configuration           the configuration
     */
    public ToJsonTransformer(Map<String, String> transformationArguments, String[] columnNames, Configuration configuration) {
        this.columnNames = columnNames;
        this.keyColumn = transformationArguments.get(KEY_COLUMN_NAME);
    }

    @Override
    public StreamInfo transform(StreamInfo streamInfo) {
        DataStream<Row> inputStream = streamInfo.getDataStream();
        SingleOutputStreamOperator<Row> outputStream = inputStream.map(this);
        String[] inColumnNames = streamInfo.getColumnNames();
        String[] outColumnNames = Arrays.copyOf(inColumnNames, inColumnNames.length + 1);
        outColumnNames[inColumnNames.length] = this.keyColumn;
        return new StreamInfo(outputStream, outColumnNames);
    }

    @Override
    public Row map(Row row) throws Exception {

        TypeInformation<Row> rowTypeInformation = getTypeInformation(row);

        JsonRowSerializationSchema convertor = new JsonRowSerializationSchema.Builder(rowTypeInformation).build();

        String json = new String(convertor.serialize(row), StandardCharsets.UTF_8);

        Row outRow = new Row(row.getArity()+1);

        for (int i = 0; i < row.getArity(); i++) {
            outRow.setField(i,row.getField(i));
        }
        outRow.setField(row.getArity(),json);
        return outRow;
    }

    private static TypeInformation<Row> getTypeInformation(Row row){
        TypeInformation<?>[] types = new TypeInformation[row.getArity()];
        String[] fieldNames = row.getFieldNames(true).stream().toArray(String[]::new);

        for (int i = 0; i < row.getArity(); i++) {
            TypeInformation<?> typeInfo = Objects.isNull(row.getField(i)) ? null
                    : TypeExtractor.createTypeInfo(row.getField(i).getClass());
            types = getRowSchema(typeInfo,
                    row.getField(i),
                    i,
                    types);
        }
        return Types.ROW_NAMED(fieldNames,types);
    }

    private static TypeInformation<?>[] getRowSchema(TypeInformation<?> info, Object value, int index, TypeInformation<?>[] types) {
        if( info == null || Types.VOID.equals(info)){
            types[index] = Types.VOID;
            return types;
        }
        else if (info.equals(Types.STRING)) {
            types[index] = Types.STRING;
            return types;
        } else if (info.equals(Types.CHAR)) {
            types[index] = Types.CHAR;
            return types;
        } else if (info.equals(Types.BOOLEAN)) {
            types[index] = Types.BOOLEAN;
            return types;
        } else if (info.equals(Types.BYTE)) {
            types[index] = Types.BYTE;
            return types;
        } else if (info.equals(Types.SHORT)) {
            types[index] = Types.SHORT;
            return types;
        } else if (info.equals(Types.INT)) {
            types[index] = Types.INT;
            return types;
        } else if (info.equals(Types.LONG)) {
            types[index] = Types.LONG;
            return types;
        } else if (info.equals(Types.FLOAT)) {
            types[index] = Types.FLOAT;
            return types;
        } else if (info.equals(Types.DOUBLE)) {
            types[index] = Types.DOUBLE;
            return types;
        } else if (info.equals(Types.BIG_DEC)) {
            types[index] = Types.BIG_DEC;
            return types;
        } else if (info.equals(Types.BIG_INT)) {
            types[index] = Types.BIG_INT;
            return types;
        } else if (info.equals(Types.SQL_DATE)) {
            types[index] = Types.SQL_DATE;
            return types;
        } else if (info.equals(Types.SQL_TIME)) {
            types[index] = Types.SQL_TIME;
            return types;
        } else if (info.equals(Types.SQL_TIMESTAMP)) {
            types[index] = Types.SQL_TIMESTAMP;
            return types;
        } else if (value instanceof LocalDate) {
            types[index] = Types.LOCAL_DATE;
            return types;
        } else if (value instanceof LocalTime) {
            types[index] = Types.LOCAL_TIME;
            return types;
        } else if (value instanceof LocalDateTime) {
            types[index] = Types.LOCAL_DATE_TIME;
            return types;
        } else if (info.equals(Types.INSTANT)) {
            types[index] = Types.INSTANT;
            return types;
        } else if (value instanceof Row) {
            types[index] = getTypeInformation((Row)value);
            return types;
        } else if (info instanceof BasicArrayTypeInfo || info instanceof PrimitiveArrayTypeInfo) {
            Object[] objects = (Object[]) value;
            Class<?> objectArrayClass = objects[0].getClass();
            types[index] = Types.PRIMITIVE_ARRAY(getRowSchema(TypeExtractor.createTypeInfo(objectArrayClass),
                    objects[0],
                    index,
                    types)[0]);
            return types;
        } else if (info instanceof ObjectArrayTypeInfo) {
            Object[] objects = (Object[]) value;
            Class<?> objectArrayClass = objects[0].getClass();
            if(objects[0] instanceof Row){
                types[index] =  Types.OBJECT_ARRAY(getTypeInformation((Row)objects[0]));
            } else {
                types[index] = Types.OBJECT_ARRAY(getRowSchema(TypeExtractor.createTypeInfo(objectArrayClass),
                        objects[0],
                        index,
                        types)[0]);
            }
            return types;
        }  else {
            throw new RuntimeException("Unsupported type information '" + info + "'.");
        }
    }

}
