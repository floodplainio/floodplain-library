package com.dexels.replication.api;

import com.dexels.pubsub.rx2.api.PubSubMessage;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;

public interface ReplicationMessageParser {
	@Deprecated
	public ReplicationMessage parseBytes(byte[] data);
	public ReplicationMessage parseBytes(PubSubMessage data);
	public ReplicationMessage parseBytes(Optional<String> source, byte[] data);
	public List<ReplicationMessage> parseMessageList(byte[] data);
	public ReplicationMessage parseStream(InputStream data);
	public ReplicationMessage parseStream(Optional<String> source, InputStream data);
	public List<ReplicationMessage> parseMessageList(Optional<String> source, InputStream data);
	public List<ReplicationMessage> parseMessageList(Optional<String> source, byte[] data);
	public byte[] serializeMessageList(List<ReplicationMessage> msg);
	public byte[] serialize(ReplicationMessage msg);
	public String describe(ReplicationMessage msg);
}

