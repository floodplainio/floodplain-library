package io.floodplain.googlesheets;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

import io.floodplain.sink.sheet.SheetSink;
import io.floodplain.sink.sheet.UpdateTuple;

public class TestSheetSink {

    String spreadsheetId = "1COkG3-Y0phnHKvwNiFpYewKhT3weEC5CmzmKkXUpPA4"; 

	@Test @Ignore
	public void testUpdate() throws IOException, GeneralSecurityException {
		
        SheetSink sink = new SheetSink();
        
//		SheetSink sink = new SheetSink();
		Map<String,Object> values = new HashMap<>();
		values.put("zipcode", "ABCD12");
		values.put("housenumber", "26");
		values.put("streetname", "Hoekse Waard");
		values.put("city", "Ter Weksel");
		values.put("countryid", "NL");
		
		sink.updateRange(spreadsheetId, "A5", sink.extractRow(values, new String[]{"streetname","housenumber","zipcode","city","countryid"})); ;

	}

	@Test
	public void testBatchUpdate() throws IOException, GeneralSecurityException {
		
        SheetSink sink = new SheetSink();
        
//		SheetSink sink = new SheetSink();
		Map<String,Object> values = new HashMap<>();
		values.put("zipcode", "ABCD12");
		values.put("housenumber", "26");
		values.put("streetname", "Hoekse Waard");
		values.put("city", "Ter Weksel");
		values.put("countryid", "NL");
		UpdateTuple ut = new UpdateTuple("A6",sink.extractRow(values, new String[]{"streetname","housenumber","zipcode","city","countryid"}));
		UpdateTuple ut2 = new UpdateTuple("A8",sink.extractRow(values, new String[]{"streetname","housenumber","zipcode","city","countryid"}));
		sink.updateRange(spreadsheetId, "A5", sink.extractRow(values, new String[]{"streetname","housenumber","zipcode","city","countryid"})); ;
		List<UpdateTuple> tuples = new ArrayList<>();
		tuples.add(ut);
		tuples.add(ut2);
		
		sink.updateRangeWithBatch(spreadsheetId, tuples);
	}

}
