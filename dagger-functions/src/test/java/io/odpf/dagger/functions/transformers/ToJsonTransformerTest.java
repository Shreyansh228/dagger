package io.odpf.dagger.functions.transformers;

import io.odpf.dagger.common.configuration.Configuration;
import junit.framework.TestCase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.types.RowUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.LinkedHashMap;

import static org.mockito.MockitoAnnotations.initMocks;

public class ToJsonTransformerTest extends TestCase {


    @Mock
    private DataStream<Row> dataStream;

    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldReturnOutputRowAsJsonString() throws Exception {

        HashMap<String, String> transformationArguments = new HashMap<>();
        transformationArguments.put("jsonColumnName", "jsonEvent");
        String[] columnNames = {"name","id","address","arrayNested"};


        LinkedHashMap<String, Integer> postionByNamesNested = new LinkedHashMap<>();
        postionByNamesNested.put("location",0);
        postionByNamesNested.put("pin",1);

        Row nestedRow = RowUtils.createRowWithNamedPositions(RowKind.INSERT, new Object[2], postionByNamesNested);
        nestedRow.setField(0,"xyz");
        nestedRow.setField(1,123456);

        LinkedHashMap<String, Integer> postionByNames = new LinkedHashMap<>();
        postionByNames.put("name",0);
        postionByNames.put("id",1);
        postionByNames.put("address",2);
        postionByNames.put("arrayNested",3);


        Row inputRow = RowUtils.createRowWithNamedPositions(RowKind.INSERT, new Object[4], postionByNames);
        inputRow.setField(0,"abcd");
        inputRow.setField(1,123);
        inputRow.setField(2,nestedRow);
        inputRow.setField(3,new Row[]{nestedRow,nestedRow});


        ToJsonTransformer featureTransformer = new ToJsonTransformer(transformationArguments, columnNames, configuration);
        Row outputRow = featureTransformer.map(inputRow);

        Assert.assertEquals(inputRow.getArity() + 1, outputRow.getArity() );
        Assert.assertEquals(inputRow.getField(0), outputRow.getField(0));
        Assert.assertEquals(inputRow.getField(1), outputRow.getField(1));

        String jsonMessage = "{\"name\":\"abcd\",\"id\":123,\"address\":{\"location\":\"xyz\",\"pin\":123456},\"arrayNested\":[{\"location\":\"xyz\",\"pin\":123456},{\"location\":\"xyz\",\"pin\":123456}]}";

        System.out.println(outputRow.getField(outputRow.getArity()-1));
        Assert.assertEquals(jsonMessage, outputRow.getField(outputRow.getArity()-1));
    }


}