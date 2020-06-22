package io.floodplain.sink.sheet;

import java.util.List;

public class UpdateTuple {
	String range;
	List<List<Object>> values;
	public UpdateTuple(String range,List<List<Object>> values) {
		this.range = range;
		this.values = values;
	}
}