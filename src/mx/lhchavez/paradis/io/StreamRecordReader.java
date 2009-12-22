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

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author lhchavez
 */
public class StreamRecordReader<K extends Writable, V extends Writable> implements RecordReader<K, V> {
    private DataInputStream is;
    private Class<? extends K> keyClass;
    private Class<? extends V> valueClass;
    private long recordCount, readCount;

    private K currentKey;
    private V currentValue;

    public StreamRecordReader(InputStream is, Class<? extends K> keyClass, Class<? extends V> valueClass) throws IOException {
        this.is = new DataInputStream(is);
        this.keyClass = keyClass;
        this.valueClass = valueClass;

        this.is.readInt();
        recordCount = this.is.readInt();
        readCount = 0;
    }

    public boolean nextKeyValue() throws IOException {
        if(readCount == recordCount) return false;

        try {
            currentKey = keyClass.newInstance();
            currentValue = valueClass.newInstance();
        } catch(Exception ex) {
            return false;
        }

        is.readInt();
        currentKey.readFields(is);
        is.readInt();
        currentValue.readFields(is);

        readCount++;

        return true;
    }

    public void close() throws IOException {
        is.close();
    }

    public K getCurrentKey() {
        return currentKey;
    }

    public V getCurrentValue() {
        return currentValue;
    }

    public float getProgress() {
        if(recordCount == 0) return 1;
        
        return readCount / (float) recordCount;
    }
}
