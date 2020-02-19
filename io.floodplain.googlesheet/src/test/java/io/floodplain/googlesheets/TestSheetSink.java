package io.floodplain.googlesheets;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import io.floodplain.sink.SheetSink;

public class TestSheetSink {

    String spreadsheetId = "1COkG3-Y0phnHKvwNiFpYewKhT3weEC5CmzmKkXUpPA4"; 

	@Test
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
}
