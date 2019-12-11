package com.dexels.immutable.impl;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.api.ImmutableMessageParser;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.immutable.json.ImmutableJSON;

//@Component(name="dexels.replication.parser.json", enabled=false)
@Component(name="dexels.immutable.parser.json", property={"name=json"})

public class JSONImmutableMessageParserImpl implements ImmutableMessageParser {

	private static final boolean INCLUDENULLVALUES = true;

	@Override
	public byte[] serialize(ImmutableMessage msg) {
		return ImmutableJSON.jsonSerializer(msg,INCLUDENULLVALUES,true);
	}

	@Override
	public String describe(ImmutableMessage msg) {
		return new String( ImmutableJSON.jsonSerializer(msg,INCLUDENULLVALUES,false));
	}

	@Activate
	public void activate() {
		ImmutableFactory.setInstance(this);
	}

	@Deactivate
	public void deactivate() {
		ImmutableFactory.setInstance(null);
	}

}
