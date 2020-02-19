package io.floodplain.sink;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.UpdateValuesResponse;
import com.google.api.services.sheets.v4.model.ValueRange;

public class SheetSink {
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static final String TOKENS_DIRECTORY_PATH = "tokens";
    private static final List<String> SCOPES = Collections.singletonList(SheetsScopes.SPREADSHEETS);

    private static final ObjectMapper objectMapper = new ObjectMapper();
	private static GoogleCredential credential;

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
        if (credential == null) {
            InputStream is = SheetSink.class.getClassLoader()
              .getResourceAsStream("sheetkey.json");
            credential = GoogleCredential.fromStream(is)
              .createScoped(Collections.singleton(SheetsScopes.SPREADSHEETS));
        }
        return credential;
    }
    
    private static Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT, String clientId, String clientSecret,String projectId) throws IOException {
        // Load client secrets.
    	try (InputStream in = SheetSink.class.getClassLoader().getResourceAsStream("credentials.json")) {
//    		JSON_FACTORY.p
//    		JacksonFactory.getDefaultInstance().createJsonParser(SheetSink.class.getClassLoader().getResourceAsStream("credentials.json")).
//    		JSON_FACTORY.createJsonParser(SheetSink.class.getClassLoader().getResourceAsStream("credentials.json")).
    		ObjectNode on = (ObjectNode) objectMapper.readTree(in);
    		on.put("client_id", clientId);
    		on.put("client_secret", clientSecret);
    		on.put("project_id", projectId);
    		ByteArrayInputStream bais = new ByteArrayInputStream(objectMapper.writeValueAsBytes(on));
            GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(bais));
            // Build flow and trigger user authorization request.
            GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                    HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                    .setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH)))
                    .setAccessType("offline")
                    .build();
            
            LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
            return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
    	}

    }
    
    public static void main(String args[]) throws IOException, GeneralSecurityException {
        // The ID of the spreadsheet to update.
        String spreadsheetId = "1COkG3-Y0phnHKvwNiFpYewKhT3weEC5CmzmKkXUpPA4"; // TODO: Update placeholder value.
        
        // The A1 notation of the values to update.

        // TODO: Assign values to desired fields of `requestBody`. All existing
        // fields will be replaced:
        SheetSink sheetSink = new SheetSink();
        
        for (int i = 1; i < 10; i++) {
            String range = "B"+i; 
            List<List<Object>> values = Arrays.asList(Arrays.asList("ooooop"+i,"eeeeeep"+i,"ieieieieiiep"));
    		sheetSink.updateRange(spreadsheetId, range,values);
		}
      }

    public List<List<Object>> extractRow(Map<String,Object> message, String[] columns) {
    	List<Object> list = new ArrayList<Object>();
    	for (String column : columns) {
    		list.add(message.get(column));
		}
    	return Arrays.asList(list);
    }
    public void updateRange(String spreadsheetId, String range, List<List<Object>> values)
			throws IOException {
        String valueInputOption = "RAW"; 
		ValueRange requestBody = new ValueRange();
		requestBody.setValues(values);
		List<List<Object>> res = sheetsService.spreadsheets().values().get(spreadsheetId,"A1").execute().getValues();
		 System.err.println(">>> "+res);
        Sheets.Spreadsheets.Values.Update request =
        sheetsService.spreadsheets().values().update(spreadsheetId, range, requestBody);
        request.setValueInputOption(valueInputOption);
        request.set("key", "");
        
         
        UpdateValuesResponse response = request.execute();

        // TODO: Change code below to process the `response` object:
        System.out.println(response);
	}
    
}
