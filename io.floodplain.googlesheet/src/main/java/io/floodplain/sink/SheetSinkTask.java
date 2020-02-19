package io.floodplain.sink;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SheetSinkTask extends SinkTask {

	private Map<String, String> props;
	private static final String SPREADSHEETID = "spreadsheetId";
	private static final String COLUMNS = "columns";

	
	private final static Logger logger = LoggerFactory.getLogger(SheetSinkTask.class);

	public String[] columns;
	private SheetSink sheetSink;
	private String spreadsheetId;

	@Override
	public String version() {
		return "0.1";
	}

	@Override
	public void start(Map<String, String> props) {
		this.props = props;
//		last_name=Willis, actor_id=83, first_name=Ben
		this.spreadsheetId = props.get(SPREADSHEETID);
//		this.clientId = Optional.ofNullable(props.get(CLIENTID)).orElse(System.getenv("GOOGLE_SHEETS_CLIENTID"));
//		this.clientSecret = Optional.ofNullable(props.get(CLIENTSECRET)).orElse(System.getenv("GOOGLE_SHEETS_CLIENTSECRET"));
//		this.projectId = Optional.ofNullable(props.get(PROJECTID)).orElse(System.getenv("GOOGLE_SHEETS_PROJECTID"));
		this.columns = props.get(COLUMNS).split(","); 

		try {
			this.sheetSink = new SheetSink();
		} catch (IOException | GeneralSecurityException e) {
			throw new RuntimeException("Problem starting sheet sink connector task",e);
		}
//		this.columns = new String[] {"actor_id","last_name","first_name"};
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		for (SinkRecord sinkRecord : records) {
			System.err.println("Record: "+sinkRecord);
			Map<String,Object> msg = (Map<String, Object>) sinkRecord.value();
			if(msg==null) {
				System.err.println("Ignoring delete of key: "+sinkRecord.key());
			} else {
				System.err.println("Inserting message: "+msg);
				Long row = (Long) msg.get("_row");
				System.err.println("Inserting row: "+row);
				String column = "B";
				int startOffset = 4;
				long currentRow = row+startOffset;
				List<List<Object>> res = sheetSink.extractRow(msg, this.columns);
				System.err.println("res: "+res);
				System.err.println("Would update: "+spreadsheetId+" : "+column+currentRow+" res: "+res);
//				sheetSink.updateRange(spreadsheetId, column+currentRow, res);
			}
//			sinkRecord.headers().iterator().next().
//			Header h = sinkRecord.headers().lastWithName("row");
//			h.
		}
		// TODO Auto-generated method stub

	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}

}
