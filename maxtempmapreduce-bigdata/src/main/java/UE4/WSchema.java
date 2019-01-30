package UE4;

import org.apache.avro.Schema;

import java.io.IOException;

public enum WSchema {
    INSTANCE;

    private String weatherRecordSchemaFile = "/avro/VSK.avsc";
    private Schema weatherRecordSchema;

    WSchema() {
        //Create the parser for the schema
        Schema.Parser parser = new Schema.Parser();

        //Create schema from .avsc file
        try {
            weatherRecordSchema = parser.parse(getClass().getResourceAsStream(weatherRecordSchemaFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Schema weatherRecordSchema() {
        return weatherRecordSchema;
    }
}
