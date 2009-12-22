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

import java.io.IOException;
import mx.lhchavez.paradis.io.RecordReader;
import mx.lhchavez.paradis.io.RecordWriter;
import mx.lhchavez.paradis.io.Writable;
import mx.lhchavez.paradis.util.Configuration;

/**
 *
 * @author lhchavez
 */
public class MapperContext<KEYIN extends Writable, VALUEIN extends Writable, KEYOUT extends Writable, VALUEOUT extends Writable> implements RecordWriter<KEYOUT, VALUEOUT> {
    public Configuration conf;
    public TaskAttemptID taskid;
    private RecordWriter<KEYOUT, VALUEOUT> writer;
    public RecordReader<KEYIN, VALUEIN> reader;

    public MapperContext(Configuration conf, TaskAttemptID taskid, RecordReader<KEYIN, VALUEIN> reader, RecordWriter<KEYOUT, VALUEOUT> writer) {
        this.conf = conf;
        this.taskid = taskid;
        this.writer = writer;
        this.reader = reader;
    }

    public void write(KEYOUT k, VALUEOUT v) throws IOException {
        writer.write(k, v);
    }

    public void close() throws IOException {
        writer.close();
    }
}