package io.floodplain.replication.impl.protobuf;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.floodplain.immutable.api.ImmutableMessage.ValueType;
import io.floodplain.pubsub.rx2.api.PubSubMessage;
import io.floodplain.replication.api.ReplicationMessage;
import io.floodplain.replication.api.ReplicationMessageParser;
import io.floodplain.replication.factory.ReplicationFactory;
import io.floodplain.replication.impl.json.JSONReplicationMessageParserImpl;
import io.floodplain.replication.impl.protobuf.impl.ProtobufReplicationMessageParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Default;
import javax.inject.Named;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

@Named("protobuffallback")
@ApplicationScoped
@Default

public class FallbackReplicationMessageParser implements ReplicationMessageParser {

    private final ReplicationMessageParser primary;
    private final ReplicationMessageParser secondary = new JSONReplicationMessageParserImpl();

    @SuppressWarnings("unused")
    private JsonProcessingException p;

    private final static Logger logger = LoggerFactory.getLogger(FallbackReplicationMessageParser.class);

    public FallbackReplicationMessageParser() {
        this("PROTOBUF".equals(System.getenv("REPLICATION_MESSAGE_FORMAT")) || "PROTOBUF".equals(System.getProperty("REPLICATION_MESSAGE_FORMAT")));
//		InvalidProtocolBufferException e;
    }

    public FallbackReplicationMessageParser(boolean useProtobuf) {
        if (useProtobuf) {
            primary = new ProtobufReplicationMessageParser();
        } else {
            primary = new JSONReplicationMessageParserImpl();
        }
    }

    private ReplicationMessageParser determineType(byte[] data) {
        if (data == null) {
            return primary;
        }
        if ((short) data[0] != ProtobufReplicationMessageParser.MAGIC_BYTE_1) {
            return secondary;
        }
        if ((short) data[1] != ProtobufReplicationMessageParser.MAGIC_BYTE_2) {
            return secondary;
        }
        return primary;
    }

    // Really need multi returns
    private InputStream determineType(InputStream data, List<ReplicationMessageParser> result) {
        PushbackInputStream pis = new PushbackInputStream(data, 2);
        try {
            byte[] pre = new byte[2];
            pis.read(pre);
            if ((short) pre[0] != ProtobufReplicationMessageParser.MAGIC_BYTE_1) {
                result.add(secondary);
            }
            if ((short) pre[1] != ProtobufReplicationMessageParser.MAGIC_BYTE_2) {
                result.add(secondary);
            }
            pis.unread(pre);
            result.add(primary);
        } catch (IOException e) {
            logger.error("Error: ", e);
            result.add(secondary);
        }
        return pis;
    }

    @Override
    public ReplicationMessage parseBytes(byte[] data) {
        return determineType(data).parseBytes(Optional.empty(), data);
    }


    @Override
    public ReplicationMessage parseBytes(Optional<String> source, byte[] data) {
        return determineType(data).parseBytes(source, data);
    }


    @Override
    public List<ReplicationMessage> parseMessageList(byte[] data) {
        return determineType(data).parseMessageList(data);
    }

    @Override
    public ReplicationMessage parseStream(InputStream data) {
        return parseStream(Optional.empty(), data);
    }

    @Override
    public ReplicationMessage parseStream(Optional<String> source, InputStream data) {
        List<ReplicationMessageParser> res = new LinkedList<>();
        InputStream is = determineType(data, res);
        ReplicationMessageParser parser = res.stream().findFirst().get();
        return parser.parseStream(is);
    }


    public List<ReplicationMessage> parseMessageList(Optional<String> source, InputStream data) {
        List<ReplicationMessageParser> res = new LinkedList<>();
        InputStream is = determineType(data, res);
        ReplicationMessageParser parser = res.stream().findFirst().get();
        return parser.parseMessageList(source, is);
    }

    @Override
    public byte[] serializeMessageList(List<ReplicationMessage> msg) {
        if (msg == null) {
            throw new NullPointerException("Describing null message list!");
        }

        return this.primary.serializeMessageList(msg);
    }

    @Override
    public byte[] serialize(ReplicationMessage msg) {
        if (msg == null) {
            throw new NullPointerException("Serializing null message!");
        }
        return this.primary.serialize(msg);
    }

    @Override
    public String describe(ReplicationMessage msg) {
        if (msg == null) {
            throw new NullPointerException("Describing null message!");
        }
        return this.primary.describe(msg);
    }


    @Override
    public List<ReplicationMessage> parseMessageList(Optional<String> source, byte[] data) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ReplicationMessage parseBytes(PubSubMessage data) {
        ReplicationMessage result = data.value() != null ? parseBytes(data.value()) : ReplicationFactory.empty().withOperation(ReplicationMessage.Operation.DELETE);
        if (ReplicationMessage.includeKafkaMetadata()) {
            return result
                    .withPartition(data.partition())
                    .withOffset(data.offset())
                    .withSource(data.topic())
                    .with("_kafkapartition", data.partition().orElse(-1), ValueType.INTEGER)
                    .with("_kafkaoffset", data.offset().orElse(-1L), ValueType.INTEGER)
                    .with("_kafkakey", data.key(), ValueType.STRING)
                    .with("_kafkatopic", data.topic().orElse(null), ValueType.STRING);
        } else {
            return result;
        }

//		return (data.value()!=null ? parseBytes(data.value()) : ReplicationFactory.empty().withOperation(Operation.DELETE))
//				.withPartition(data.partition())
//				.withOffset(data.offset())
//				.withSource(data.topic())
//				.with("_kafkapartition", data.partition().orElse(-1), "integer")
//				.with("_kafkaoffset", data.offset().orElse(-1L), "long")
//				.with("_kafkakey", data.key(), "string")
//				.with("_kafkatopic",data.topic().orElse(null),"string");
    }

}
