package com.dexels.mongodb.sink.customcodecs;

import org.bson.BsonReader;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.immutable.api.customtypes.CoordinateType;

public class CoordinateTypeCodec implements CollectibleCodec<CoordinateType> {

    private static final Logger logger = LoggerFactory.getLogger(CoordinateType.class);

    private CodecRegistry codecRegistry;
    private BsonTypeClassMap bsonTypeClassMap;
    private boolean addMongoElementTypeField;

    public CoordinateTypeCodec(final CodecRegistry codecRegistry, final BsonTypeClassMap bsonTypeClassMap) {
        this.codecRegistry = codecRegistry;
        this.bsonTypeClassMap = bsonTypeClassMap;
        this.addMongoElementTypeField = true;
    }

    public CoordinateTypeCodec(CodecRegistry registry, BsonTypeClassMap bsonTypeClassMap, boolean addMongoElementIdField) {
        this(registry, bsonTypeClassMap);
        this.addMongoElementTypeField = addMongoElementIdField;
    }

    @Override
    public void encode(BsonWriter writer, CoordinateType coor, EncoderContext encoderContext) {
        writer.writeStartDocument();

        // Document doc = new Document();
        // doc.put("_type", "coordinate");
        // doc.put("type", "Point");
        // doc.put("coordinates", (List) coordinatesList);

        if (addMongoElementTypeField) {
            writer.writeName("_type");
            writer.writeString("coordinate");
        }

        writer.writeName("type");
        writer.writeString("Point");

        writer.writeName("coordinates");
        writer.writeStartArray();
        writer.writeDouble(coor.getLongitude());
        writer.writeDouble(coor.getLatitude());

        writer.writeEndArray();
        writer.writeEndDocument();

    }

    @Override
    public Class<CoordinateType> getEncoderClass() {
        return CoordinateType.class;
    }

    @SuppressWarnings("finally")
    @Override
    public CoordinateType decode(BsonReader reader, DecoderContext decoderContext) {
        CoordinateType coor = null;

        reader.readStartDocument();
        reader.readName(); // read the name "_type"
        reader.skipValue();

        reader.readName(); // read the name "type"
        reader.skipValue();

        reader.readName(); // read the name "coordinate"
        reader.readStartArray();

        try {
            coor = new CoordinateType(reader.readDouble(), reader.readDouble());
        } catch (Exception e) {
            logger.error("Could not decode CoordinateType. {}", e);
            coor = null;
        } finally {
            reader.readEndArray();
            reader.readEndDocument();

            return coor;
        }
    }

    @Override
    public boolean documentHasId(CoordinateType arg0) {
        return false;
    }

    @Override
    public CoordinateType generateIdIfAbsentFromDocument(CoordinateType arg0) {
        return null;
    }

    @Override
    public BsonValue getDocumentId(CoordinateType arg0) {
        return null;
    }

}
