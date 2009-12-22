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

package mx.lhchavez.paradis.io;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 *
 * @author lhchavez
 */
public class StreamRecordWriter<K extends Writable, V extends Writable> implements RecordWriter<K,V>{
    private RandomAccessFile output;
    private boolean closed;
    private long startPtr;
    private long recordCount;

    public StreamRecordWriter() {
        closed = true;
    }

    public void setOutput(RandomAccessFile randomAccessFile) throws IOException {
        if(!closed)
            this.output.close();
        this.output = randomAccessFile;

        startPtr = output.getFilePointer();

        output.writeInt(0);
        output.writeInt(0);

        recordCount = 0;
        closed = false;
    }

    public void close() throws IOException {
        long curPtr = output.getFilePointer();
        output.seek(startPtr);
        output.writeInt((int) (curPtr - startPtr - 4));
        output.writeInt((int) recordCount);

        output.seek(curPtr);
        output.close();

        closed = true;
    }

    public void write(K k, V v) throws IOException {
        long lenPtr, curPtr;

        lenPtr = output.getFilePointer();
        output.writeInt(0);
        k.write(output);
        curPtr = output.getFilePointer();
        output.seek(lenPtr);
        output.writeInt((int) (curPtr - lenPtr - 4));
        output.seek(curPtr);

        lenPtr = output.getFilePointer();
        output.writeInt(0);
        v.write(output);
        curPtr = output.getFilePointer();
        output.seek(lenPtr);
        output.writeInt((int) (curPtr - lenPtr - 4));
        output.seek(curPtr);

        ++recordCount;
    }

}
