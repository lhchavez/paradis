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
import mx.lhchavez.paradis.io.Writable;
import mx.lhchavez.paradis.io.WritableComparable;

public class Mapper<KEYIN extends Writable, VALUEIN extends Writable, KEYOUT extends WritableComparable, VALUEOUT extends Writable> {

    public void run(MapperContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) throws IOException, InterruptedException {
        setup(context);

        while(context.reader.nextKeyValue()) {
            map(context.reader.getCurrentKey(), context.reader.getCurrentValue(), context);
        }

        cleanup(context);
    }

    protected void setup(MapperContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) {}
    protected void map(KEYIN key, VALUEIN value, MapperContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) throws IOException, InterruptedException {}
    protected void cleanup(MapperContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) {}
}
