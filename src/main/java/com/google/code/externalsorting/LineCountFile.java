package com.google.code.externalsorting;

import java.io.File;

public final class LineCountFile {
    private final File file;
    private final int lineCount;

    public LineCountFile(final File file, final int lineCount) {
        this.file = file;
        this.lineCount = lineCount;
    }

    public File getFile() {
        return file;
    }

    public int getLineCount() {
        return lineCount;
    }
}
