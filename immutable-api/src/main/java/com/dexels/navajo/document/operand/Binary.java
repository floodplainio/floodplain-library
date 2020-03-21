package com.dexels.navajo.document.operand;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.ZipOutputStream;

public class Binary {

    public final byte[] data;

    public Binary(byte[] data) {
        this.data = data;
    }

    public Binary() {
        data = null;
    }

    public Binary(InputStream openStream) throws IOException {
        data = readBytes(openStream);
    }

    public Binary(File u) throws IOException {
        this(new FileInputStream(u));
    }

    public String guessContentType() {
        return "";
    }

    public byte[] getData() {
        return data;
    }

    public boolean isEqual(Binary b) {
        if(data==null) {
            return b.data==null;
        }
        if(b.data==null) {
            return false;
        }
        return data.equals(b.data);
    }

    public int getLength() {
        return data == null ? 0 : data.length;
    }

    public InputStream getDataAsStream() {
        return data == null ? new ByteArrayInputStream(new byte[]{}) : new ByteArrayInputStream(data);
    }

    private static byte[] readBytes( InputStream stream ) throws IOException {
        if (stream == null) return new byte[] {};
        byte[] buffer = new byte[1024];
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        boolean error = false;
        try {
            int numRead = 0;
            while ((numRead = stream.read(buffer)) > -1) {
                output.write(buffer, 0, numRead);
            }
        } catch (IOException e) {
            error = true; // this error should be thrown, even if there is an error closing stream
            throw e;
        } catch (RuntimeException e) {
            error = true; // this error should be thrown, even if there is an error closing stream
            throw e;
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                if (!error) throw e;
            }
        }
        output.flush();
        return output.toByteArray();
    }

    public void write(OutputStream zo) throws IOException {
        zo.write(data);
    }

    public void setMimeType(String mt) {
    }
}
