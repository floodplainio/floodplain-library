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

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.Sheets.Spreadsheets.Values.BatchUpdate;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SheetSink {
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
	private static final String TOKENS_DIRECTORY_PATH = "tokens";
	private static final List<String> SCOPES = Collections.singletonList(SheetsScopes.SPREADSHEETS);

	private static final ObjectMapper objectMapper = new ObjectMapper();
	private static GoogleCredential credential;
	private final static Logger logger = LoggerFactory.getLogger(SheetSink.class);

	private Sheets sheetsService;

	public SheetSink() throws IOException, GeneralSecurityException {
		sheetsService = createSheetsService();
	}

	private static Sheets createSheetsService() throws IOException, GeneralSecurityException {
		HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
		JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
		final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();

		HttpRequestInitializer credential = getCredential(); // getCredentials(HTTP_TRANSPORT,clientId,clientSecret,projectId);
//        HttpRequestInitializer credential = req->req. .getUrl().put("key", "");

		return new Sheets.Builder(httpTransport, jsonFactory, credential)
				.setApplicationName("Google-SheetsSample/0.1")
				.build();
	}

	public static Credential getCredential() throws IOException {
		String path = Optional.ofNullable(System.getenv("GOOGLE_SHEETS_CREDENTIAL_PATH")).orElse("/kafka/credentials.json");
		if (credential == null) {
			try (InputStream is = new FileInputStream(path)) {
				credential = GoogleCredential.fromStream(is)
						.createScoped(Collections.singleton(SheetsScopes.SPREADSHEETS));
			}
		}
		return credential;
	}

	public List<List<Object>> extractRow(Map<String, Object> message, String[] columns) {
		List<Object> list = new ArrayList<Object>();
		for (String column : columns) {
			list.add(message.get(column));
		}
		return Arrays.asList(list);
	}

	public void deleteRows(String spreadsheetId, String range) throws IOException {
		ClearValuesRequest requestBody = new ClearValuesRequest();

		Sheets.Spreadsheets.Values.Clear request =
				sheetsService.spreadsheets().values().clear(spreadsheetId, range, requestBody);

		ClearValuesResponse response = request.execute();
	}

	public void clear(String spreadsheetId, List<String> ranges) throws IOException {
		BatchClearValuesRequest requestBody = new BatchClearValuesRequest();
		requestBody.setRanges(ranges);
		Sheets.Spreadsheets.Values.BatchClear request =
				sheetsService.spreadsheets().values().batchClear(spreadsheetId, requestBody);
		BatchClearValuesResponse response = request.execute();
//		sheetsService.spreadsheets().batchUpdate(spreadsheetId,pp);

	}
	public List<List<Object>> getRange(String spreadsheetId, String range) throws IOException {
		return sheetsService.spreadsheets().values().get(spreadsheetId,range).execute().getValues();
	}

    public void updateRange(String spreadsheetId, String range, List<List<Object>> values)
			throws IOException {
        String valueInputOption = "RAW"; 
		ValueRange requestBody = new ValueRange();
		requestBody.setValues(values);
		logger.info("Adding {} rows to sheet",values.size());
//		List<List<Object>> res = sheetsService.spreadsheets().values().get(spreadsheetId,"A1").execute().getValues();
//		 System.err.println(">>> "+res);
        Sheets.Spreadsheets.Values.Update request =
        sheetsService.spreadsheets().values().update(spreadsheetId, range, requestBody);
        request.setValueInputOption(valueInputOption);
        request.set("key", "");
        
         
        UpdateValuesResponse response = request.execute();

        // TODO: Change code below to process the `response` object:
        System.out.println(response);
	}
    
  	public void updateRangeWithBatch(String spreadsheetId, List<UpdateTuple> tuples) throws IOException {

    	String valueInputOption = "RAW"; 
		List<ValueRange> data = new ArrayList<>();
		tuples.forEach(tuple->{
			data.add(new ValueRange()
			        .setRange(tuple.range)
			        .setValues(tuple.values));
		});
		// Additional ranges to update ...

		BatchUpdateValuesRequest body = new BatchUpdateValuesRequest()
		        .setValueInputOption(valueInputOption)
		        .setData(data);

		BatchUpdate request = sheetsService.spreadsheets().values().batchUpdate(spreadsheetId, 
				body);
		request.set("key", "");
         
        BatchUpdateValuesResponse response = request.execute();

		// TODO Deal with the response
        System.out.println(response);
	}

    
}
