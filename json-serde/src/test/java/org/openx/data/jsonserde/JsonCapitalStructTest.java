package org.openx.data.jsonserde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;
import org.openx.data.jsonserde.json.JSONObject;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

/**
 * Created by nathan.demaria on 5/22/2015.
 */
public class JsonCapitalStructTest {
    static JsonSerDe instance;

    @Before
    public void setUp() throws Exception {
        initialize();
    }

    static public void initialize() throws Exception {
        instance = new JsonSerDe();
        Configuration conf = null;
        Properties tbl = new Properties();
        // from google video API
        tbl.setProperty(Constants.LIST_COLUMNS, "fieldname");
        tbl.setProperty(Constants.LIST_COLUMN_TYPES, "struct<keya:string>".toLowerCase());

        instance.initialize(conf, tbl);
    }

    @Test
    public void testCapitalKeyStruct() throws Exception {
        /*
         * Confirms behavior that if you have multiple keys with the same
         * name (case insensitive) in a struct, the second will overwrite it
         */
        Writable w = new Text("{\"fieldname\": {\"keya\": \"firstValue\", \"Keya\": \"secondValue\"}}");

        JSONObject result = (JSONObject) instance.deserialize(w);

        StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();

        StructField fieldnameStructField = soi.getStructFieldRef("fieldname");
        Object fieldname = soi.getStructFieldData(result, fieldnameStructField);
        assertEquals(fieldnameStructField.getFieldObjectInspector().getCategory(), ObjectInspector.Category.STRUCT);

        StructObjectInspector fieldnameSOI = (StructObjectInspector) fieldnameStructField.getFieldObjectInspector();

        StructField ksf = fieldnameSOI.getStructFieldRef("keya");
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector) ksf.getFieldObjectInspector();
        SettableStringObjectInspector strOI = (SettableStringObjectInspector) poi;

        Object keya = fieldnameSOI.getStructFieldData(fieldname, ksf);
        Text value = strOI.getPrimitiveWritableObject(keya);

        assertEquals("secondValue", value.toString());
    }
}
