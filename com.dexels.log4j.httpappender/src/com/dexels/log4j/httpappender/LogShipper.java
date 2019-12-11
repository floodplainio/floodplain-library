package com.dexels.log4j.httpappender;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyStore;
import java.util.Date;
import java.util.zip.DeflaterOutputStream;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public class LogShipper {
    public static final int CONNECT_TIMEOUT = 5000;

    private KeyStore keyStore;
    private URL url;

    private boolean useHttps = false;
    private boolean useCompression = true;
    private boolean enabled = false;
    private long timeDiff;

    public void setRemoteLoggerURL(String loggerURL)  {
        this.timeDiff = 0L;
      
        if (loggerURL != null && !loggerURL.equals("")) {
            try {
                this.url = new URL(loggerURL);
            } catch (IOException e) {
                return;
            }
          
            useHttps = loggerURL.startsWith("https");
            System.out.println("Activated LogShipper");
            
            // Ship empty event to determine the time diff
            enabled = true;
            try {
                ship("[]");
            } catch (IOException e) {
            }
        }
    }

    public void ship(String logObject) throws IOException {
        if (!enabled) {
            return;
        }
        if (logObject == null || logObject.equals("")) {
            return;
        }

        HttpURLConnection con = null;
        con = (HttpURLConnection) url.openConnection();
        if (useHttps) {
            HttpsURLConnection httpsCon = (HttpsURLConnection) con;

            if (keyStore != null) {
                try {
                    String tmfAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
                    TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
                    tmf.init(keyStore);
                    SSLContext context = SSLContext.getInstance("TLS");
                    context.init(null, tmf.getTrustManagers(), null);
                    httpsCon.setSSLSocketFactory(context.getSocketFactory());
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }

        con.setRequestMethod("POST");
        con.setConnectTimeout(CONNECT_TIMEOUT);
        con.setDoOutput(true);
        con.setDoInput(true);
        con.setUseCaches(false);
        con.setRequestProperty("Content-type", "text/json; charset=UTF-8");
        con.setRequestProperty("Connection", "Keep-Alive");
        con.setChunkedStreamingMode(1024);
        con.setRequestProperty("Transfer-Encoding", "chunked");
      
        if (useCompression) {
          
            con.setRequestProperty("Content-Encoding", "jzlib");
            con.setRequestProperty("Accept-Encoding", "jzlib");
            BufferedWriter out = null;
            try {
             
                out = new BufferedWriter(new OutputStreamWriter(new DeflaterOutputStream(con.getOutputStream()), "UTF-8"));
                out.write(logObject);
                
                
            } finally {
                if (out != null) {
                    try {
                        out.flush();
                        out.close();
                        Long serverTime = Long.valueOf(con.getHeaderField("received-at"));
                        this.timeDiff = serverTime -  new Date().getTime();
                        if (con != null) {
                            con.disconnect();
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else {
            con.setRequestProperty("noCompression", "true");
            BufferedWriter os = null;
            try {
                os = new BufferedWriter(new OutputStreamWriter(con.getOutputStream(), "UTF-8"));
                os.write(logObject);
               
            } finally {
                if (os != null) {
                    try {
                       
                        os.flush();
                        os.close();
                        Long serverTime = Long.valueOf(con.getHeaderField("received-at"));
                        this.timeDiff = serverTime - new Date().getTime();
                        
                        if (con != null) {
                            con.disconnect();
                        }
                       
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }

    public long getTimeDiff() {
        return timeDiff;
    }
    

    public static void main(String[] args) {
        LogShipper s = new LogShipper();
        s.setRemoteLoggerURL("https://club-test.sportlink.com/logserver");
        try {
            s.ship("[]");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
