package org.openx.data.jsonserde;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
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
public class JsonCapitalTest {
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
        tbl.setProperty(Constants.LIST_COLUMN_TYPES, "map<string,string>".toLowerCase());

        instance.initialize(conf, tbl);
    }

    @Test
    public void testCapitalKey() throws Exception {
        //Test that you can have two keys in a map that differ only by case sensitivity
        Writable w = new Text("{\"fieldname\": {\"keya\": \"value\", \"Keya\": \"value\"}}");

        JSONObject result = (JSONObject) instance.deserialize(w);

        StructObjectInspector soi = (StructObjectInspector) instance.getObjectInspector();

        StructField sfr = soi.getStructFieldRef("fieldname");

        assertEquals(sfr.getFieldObjectInspector().getCategory(), ObjectInspector.Category.MAP);

        MapObjectInspector moi = (MapObjectInspector) sfr.getFieldObjectInspector();

        Object val =  soi.getStructFieldData(result, sfr) ;

        assertEquals(2, moi.getMapSize(val));

    }
}
