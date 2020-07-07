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
	
	private void putWithSingle(Collection<SinkRecord> records) {
		for (SinkRecord sinkRecord : records) {
			System.err.println("Record: "+sinkRecord);
			Map<String,Object> toplevel = (Map<String, Object>) sinkRecord.value();
			Map<String,Object> msg = (Map<String, Object>) toplevel.get("payload");
			if(msg==null) {
				logger.info("Ignoring delete of key: {} deletions aren't supported",sinkRecord.key());
			} else {
				Long row = (Long) msg.get("_row");
				String column = "B";
				int startOffset = 4;
				long currentRow = row+startOffset;
				List<List<Object>> res = sheetSink.extractRow(msg, this.columns);
				logger.info("Would update: {} : {} res: {}",spreadsheetId,column+currentRow,res);
				try {
					sheetSink.updateRange(spreadsheetId, column+currentRow, res);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	
	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}

}
