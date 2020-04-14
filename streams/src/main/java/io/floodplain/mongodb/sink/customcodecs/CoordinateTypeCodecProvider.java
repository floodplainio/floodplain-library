package io.floodplain.mongodb.sink.customcodecs;

import io.floodplain.immutable.api.customtypes.CoordinateType;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

public class CoordinateTypeCodecProvider implements CodecProvider {

    private static final BsonTypeClassMap DEFAULT_BSON_TYPE_CLASS_MAP = new BsonTypeClassMap();

    private final BsonTypeClassMap bsonTypeClassMap;
    private boolean addMongoElementTypeField;

    public CoordinateTypeCodecProvider() {
        this.bsonTypeClassMap = DEFAULT_BSON_TYPE_CLASS_MAP;
    }

    public CoordinateTypeCodecProvider(final BsonTypeClassMap bsonTypeClassMap) {
        this.bsonTypeClassMap = bsonTypeClassMap;
        this.addMongoElementTypeField = true;
    }

    public CoordinateTypeCodecProvider(boolean addMongoElementTypeField) {
        this();
        this.addMongoElementTypeField = addMongoElementTypeField;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (CoordinateType.class.isAssignableFrom(clazz)) {
            // construct MessageCodec with a CodecRegistry
            return (Codec<T>) new CoordinateTypeCodec(registry, bsonTypeClassMap, addMongoElementTypeField);
        }

        // CodecProvider returns null if it's not a provider for the requresed Class
        return null;
    }

}
