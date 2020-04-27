package com.google.code.externalsorting;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * This is essentially a thin wrapper on top of a BufferedReader... which keeps
 * the last line in memory.
 *
 */
public final class BinaryFileBuffer implements IOStringStack {
    public BinaryFileBuffer(BufferedReader r, int size) throws IOException {
        this.fbr = r;
        this.size = size;
        reload();
    }

    public BinaryFileBuffer(BufferedReader r) throws IOException {
        this(r, 0);
    }

    public void close() throws IOException {
        this.fbr.close();
    }

    public boolean empty() {
        return this.cache == null;
    }

    public String peek() {
        return this.cache;
    }

    public String pop() throws IOException {
        String answer = peek().toString();// make a copy
        reload();
        size -= 1;
        return answer;
    }

    @Override
    public int size() {
        return size;
    }

    private void reload() throws IOException {
        this.cache = this.fbr.readLine();
    }

    private final BufferedReader fbr;

    private String cache;

    private int size;

}