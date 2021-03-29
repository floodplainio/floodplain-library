/**
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

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class SheetSinkTask extends SinkTask {

	public static final String SPREADSHEETID = "spreadsheetId";
	public static final String COLUMNS = "columns";
	public static final String TOPICS = "topics";
	public static final String STARTROW = "startRow";
	public static final String STARTCOLUMN = "startColumn";

	
	private final static Logger logger = LoggerFactory.getLogger(SheetSinkTask.class);

	public String[] columns;
	private SheetSink sheetSink;
	private String spreadsheetId;
	private int startRow;
	private String startColumn;

	private final AtomicLong totalInTransaction = new AtomicLong(0);

	@Override
	public String version() {
		return "0.1";
	}

	@Override
	public void start(Map<String, String> props) {
		logger.info("Starting Sheet connector: {}",props);
		this.spreadsheetId = props.get(SPREADSHEETID);
		this.columns = props.get(COLUMNS).split(",");
		this.startRow = Optional.of(props.get(STARTROW)).map(Integer::parseInt).orElse(1);
		this.startColumn = Optional.of(props.get(STARTCOLUMN)).orElse("A");
		try {
			this.sheetSink = new SheetSink();
		} catch (IOException | GeneralSecurityException e) {
			throw new RuntimeException("Problem starting sheet sink connector task",e);
		}
	}

	/**
	 * For testing
	 * @return The sheet sink service
	 */
	public SheetSink getSheetSink() {
		return this.sheetSink;
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		List<UpdateTuple> tuples = extractTuples(records);
		long now = System.currentTimeMillis();
		try {
			sheetSink.updateRangeWithBatch(spreadsheetId, tuples);
		} catch (IOException e1) {
			logger.error("Error: ", e1);
		}
		long elapsed = System.currentTimeMillis() - now;
		logger.info("Update took: {} total: {}",elapsed,totalInTransaction.addAndGet(elapsed));
	}

	private List<UpdateTuple> extractTuples(Collection<SinkRecord> records) {
		LinkedHashMap<Integer,UpdateTuple> result = new LinkedHashMap<>();
		logger.info("Inserting {} records",records.size());
		for (SinkRecord sinkRecord : records) {
			Map<String,Object> toplevel = (Map<String, Object>) sinkRecord.value();
			// TODO figure this out
			if(toplevel ==null) {
				logger.info("Ignoring delete of key: {}", sinkRecord.key());
			} else {
				Integer row = (Integer) toplevel.get("_row");
				if(row==null) {
					throw new IllegalArgumentException("Invalid message for Google Sheets: Every message should have an int or long field named: '_row', marking the row where it should be inserted ");
				}
				List<List<Object>> res = sheetSink.extractRow(toplevel, this.columns);
				int currentRow = row+startRow;
				UpdateTuple ut = new UpdateTuple(startColumn+currentRow, res);
				result.put(currentRow,ut);
			}
		}
		return new ArrayList<>(result.values());
	}

	@Override
	public void stop() {
		// noop
	}

}
