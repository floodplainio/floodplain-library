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
package io.floodplain.googlesheets;

import io.floodplain.sink.sheet.SheetSink;
import io.floodplain.sink.sheet.UpdateTuple;
import org.junit.Test;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestSheetSink {

		final String spreadsheetId = "1COkG3-Y0phnHKvwNiFpYewKhT3weEC5CmzmKkXUpPA4";

	@Test
	public void testBatchUpdate() throws IOException, GeneralSecurityException {
		
        SheetSink sink = new SheetSink();
        
		Map<String,Object> values = new HashMap<>();
		values.put("zipcode", "ABCD12");
		values.put("housenumber", "26");
		values.put("streetname", "Hoekse Waard");
		values.put("city", "Ter Weksel");
		values.put("countryid", "NL");
		UpdateTuple ut = new UpdateTuple("A6",sink.extractRow(values, new String[]{"streetname","housenumber","zipcode","city","countryid"}));
		UpdateTuple ut2 = new UpdateTuple("A8",sink.extractRow(values, new String[]{"streetname","housenumber","zipcode","city","countryid"}));
		sink.updateRange(spreadsheetId, "A5", sink.extractRow(values, new String[]{"streetname","housenumber","zipcode","city","countryid"}));
		List<UpdateTuple> tuples = new ArrayList<>();
		tuples.add(ut);
		tuples.add(ut2);
		
		sink.updateRangeWithBatch(spreadsheetId, tuples);
	}

}
