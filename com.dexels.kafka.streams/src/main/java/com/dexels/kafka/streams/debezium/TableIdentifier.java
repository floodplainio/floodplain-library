package com.dexels.kafka.streams.debezium;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.replication.api.ReplicationMessage;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class TableIdentifier {
	public final String deployment;
	public final Optional<String> tenant;
	public final Optional<String> databaseInstance;
	public final String table;
	public final ImmutableMessage keyMessage;
	public final String combinedKey;
	public final List<String> fields;
	
	
	
	public TableIdentifier(String tableId, ImmutableMessage keyMessage, List<String> fields, boolean appendTenant, boolean appendSchema) {
//		dev.knvb_bbbb04b.sport
//		demo-dvd-2.public.inventory
//		logger.info("TableId: {}",tableId);
		final String[] primary = tableId.split("\\.");
		this.keyMessage = keyMessage;
		List<String> l = new LinkedList<>(fields);
		if(appendTenant) {
			l.add(0, "tenant");
		}
		if(appendSchema) {
			l.add(0, "instance");
		}
		this.fields = Collections.unmodifiableList(l);

		if(primary[1].indexOf("_")==-1) {
			tenant = Optional.empty();
			table = primary[2];
			deployment = primary[0];
			databaseInstance = Optional.empty();
			combinedKey = fields.stream()
					.map(field->keyMessage.columnValue(field).toString())
					.collect(Collectors.joining(ReplicationMessage.KEYSEPARATOR));
		} else {
			deployment = primary[0];
			final String[] secondary = primary[1].split("_");
			tenant = Optional.of(secondary[0].toUpperCase());
			databaseInstance = Optional.of(secondary[1].toUpperCase());
			table = primary[2];
			final String primarykeys = fields.stream()
				.map(field->keyMessage.columnValue(field).toString())
				.collect(Collectors.joining(ReplicationMessage.KEYSEPARATOR));
			StringBuilder sb = new StringBuilder();
			if(appendTenant) {
				if(!tenant.isPresent()) {
					throw new IllegalArgumentException("Error creating table identifier: appendTenant is supplied, but there is no tenant present.");
				}
				sb.append(tenant.get());
				sb.append(ReplicationMessage.KEYSEPARATOR);
			}
			if(appendSchema) {
				if(!databaseInstance.isPresent()) {
					throw new IllegalArgumentException("Error creating table identifier: appendSchema is supplied, but there is no schema present.");
				}
				sb.append(databaseInstance.get());
				sb.append(ReplicationMessage.KEYSEPARATOR);
			}
			sb.append(primarykeys);
			combinedKey = sb.toString();
//			(appendTenant ? tenant + ReplicationMessage.KEYSEPARATOR : "") + databaseInstance + ReplicationMessage.KEYSEPARATOR + 
//				primarykeys;
		}

	}
}
