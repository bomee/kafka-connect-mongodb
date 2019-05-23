package org.apache.kafka.connect.mongodb.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonTimestamp;
import org.bson.Document;

/**
 * Struct converter who stores mongodb document as Json String.
 *
 * @author André Ignacio
 */
public class JsonStructConverter implements StructConverter {

    @Override
    public Struct toStruct(Document document, Schema schema) {
        final Struct messageStruct = new Struct(schema);
        final BsonTimestamp bsonTimestamp = (BsonTimestamp) document.get("ts");
        final Integer seconds = bsonTimestamp.getTime();
        final Integer order = bsonTimestamp.getInc();
        messageStruct.put("ts_seconds", seconds);
        messageStruct.put("ts_inc", order);
        messageStruct.put("op", document.get("op"));
        messageStruct.put("ns", document.get("ns"));
        messageStruct.put("o", ((Document) document.get("o")).toJson());
        if (document.get("o2") != null) {
            messageStruct.put("o2", ((Document) document.get("o2")).toJson());
        }

        return messageStruct;
    }

    @Override
    public Schema createSchema(String name) {
        return SchemaBuilder
                .struct()
                .name(name)
                .field("ts_seconds", Schema.OPTIONAL_INT32_SCHEMA)
                .field("ts_inc", Schema.OPTIONAL_INT32_SCHEMA)
                .field("op", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ns", Schema.OPTIONAL_STRING_SCHEMA)
                .field("o", Schema.OPTIONAL_STRING_SCHEMA)
                .field("o2", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

}
