/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.floodplain.sink.sheet;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.*;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SheetSinkTask extends SinkTask {

	private Map<String, String> props;
	public static final String SPREADSHEETID = "spreadsheetId";
	public static final String COLUMNS = "columns";
	public static final String TOPIC = "topic";
	public static final String STARTROW = "startRow";
	public static final String STARTCOLUMN = "startColumn";

	
	private final static Logger logger = LoggerFactory.getLogger(SheetSinkTask.class);

	public String[] columns;
	private SheetSink sheetSink;
	private String spreadsheetId;
	private int startRow;
	private String startColumn;

	@Override
	public String version() {
		return "0.1";
	}

	@Override
	public void start(Map<String, String> props) {
		this.props = props;
		logger.info("Starting Sheet connector: {}",props);
		this.spreadsheetId = props.get(SPREADSHEETID);
		this.columns = props.get(COLUMNS).split(",");
		this.startRow = Optional.of(props.get(STARTROW)).map(e->Integer.parseInt(e)).orElse(1);
		this.startColumn = Optional.of(props.get(STARTCOLUMN)).orElse("A");
		try {
			this.sheetSink = new SheetSink();
		} catch (IOException | GeneralSecurityException e) {
			throw new RuntimeException("Problem starting sheet sink connector task",e);
		}
	}

	/**
	 * For testing
	 * @return
	 */
	public SheetSink getSheetSink() {
		return this.sheetSink;
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		List<UpdateTuple> tuples = extractTuples(records);
		try {
			sheetSink.updateRangeWithBatch(spreadsheetId, tuples);
		} catch (IOException e1) {
			logger.error("Error: ", e1);
		}
	}

	private List<UpdateTuple> extractTuples(Collection<SinkRecord> records) {
		List<UpdateTuple> result = new ArrayList<UpdateTuple>();
		for (SinkRecord sinkRecord : records) {
			Map<String,Object> toplevel = (Map<String, Object>) sinkRecord.value();
			// TODO figure this out
			Map<String,Object> msg = (Map<String, Object>) toplevel; // .get("payload");
			if(msg==null) {
				logger.info("Ignoring delete of key: {}", sinkRecord.key());
			} else {
				Integer row = (Integer) msg.get("_row");
				if(row==null) {
					throw new IllegalArgumentException("Invalid message for Google Sheets: Every message should have an int or long field named: '_row', marking the row where it should be inserted ");
				}
				int currentRow = row+startRow;
				List<List<Object>> res = sheetSink.extractRow(msg, this.columns);
				logger.warn("res: "+res);
				logger.warn("Would update: {} : {} res: {}",spreadsheetId,startColumn+currentRow,res);
				UpdateTuple ut = new UpdateTuple(startColumn+currentRow, res);
				result.add(ut);
			}
		}
		
		return result;
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}

}
