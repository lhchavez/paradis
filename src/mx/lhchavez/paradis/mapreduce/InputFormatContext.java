/*
 * Copyright (c) 2009, Luis Hector Chavez <lhchavez@lhchavez.com>
 * 
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package mx.lhchavez.paradis.mapreduce;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import mx.lhchavez.paradis.io.RecordWriter;
import mx.lhchavez.paradis.io.StreamRecordWriter;
import mx.lhchavez.paradis.io.Writable;
import mx.lhchavez.paradis.util.Configuration;

/**
 * @author lhchavez
 */

public class InputFormatContext<KEYIN extends Writable, VALUEIN extends Writable> implements RecordWriter<KEYIN, VALUEIN> {
    public Configuration conf;
    public long splitCount;
    private StreamRecordWriter<KEYIN, VALUEIN> writer;
    private File inputSplitDirectory;
    private boolean closed;

    public InputFormatContext(Configuration conf, File inputSplitDirectory) {
        this.conf = conf;
        this.splitCount = 0;
        this.writer = new StreamRecordWriter<KEYIN, VALUEIN>();
        this.inputSplitDirectory = inputSplitDirectory;
        this.closed = true;
    }

    public void openInputSplit() throws IOException {
        if(!closed)
            close();

        writer.setOutput(new RandomAccessFile(inputSplitDirectory.getAbsolutePath() + File.separator + splitCount, "rw"));
        splitCount++;
        closed = false;
    }

    public void write(KEYIN k, VALUEIN v) throws IOException {
        writer.write(k, v);
    }

    public void close() throws IOException {
        writer.close();
        closed = true;
    }

    public long getSplitCount() {
        return splitCount;
    }
}
