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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author lhchavez
 */
public class WritableInt implements WritableComparable<WritableInt> {
    private int value;

    public WritableInt() {
        this(0);
    }

    public WritableInt(int value) {
        this.value = value;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(value);
    }

    public void readFields(DataInput in) throws IOException {
        value = in.readInt();
    }

    public int getValue() {
        return value;
    }

    public int compareTo(WritableInt o) {
        if(value == o.value) return 0;
        else if(value < o.value) return -1;
        return 1;
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
