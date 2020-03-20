package com.dexels.navajo.document.operand;

public class Binary {

    public final byte[] data;

    public Binary(byte[] data) {
        this.data = data;
    }

    public Binary() {
        data = null;
    }

    public String guessContentType() {
        return "";
    }

    public byte[] getData() {
        return data;
    }
}
