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
public class WritableThrowable implements Writable {

    private Throwable ex;

    public WritableThrowable() {
    }

    public WritableThrowable(Throwable ex) {
        this.ex = ex;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(ex.getClass().getName() + ": " + ex.getMessage());
        StackTraceElement[] stackTrace = ex.getStackTrace();
        out.writeInt(stackTrace.length);

        for (StackTraceElement ste : stackTrace) {
            out.writeUTF(ste.getClassName());
            out.writeUTF(ste.getMethodName());
            out.writeUTF(ste.getFileName());
            out.writeInt(ste.getLineNumber());
        }

        out.writeBoolean(ex.getCause() != null);

        if (ex.getCause() != null) {
            WritableThrowable cause = new WritableThrowable(ex.getCause());
            cause.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        ex = new Throwable(in.readUTF());
        StackTraceElement[] stackTrace = new StackTraceElement[in.readInt()];
        for (int i = 0; i < stackTrace.length; i++) {
            stackTrace[i] = new StackTraceElement(in.readUTF(), in.readUTF(), in.readUTF(), in.readInt());
        }
        ex.setStackTrace(stackTrace);

        if (in.readBoolean()) {
            WritableThrowable cause = new WritableThrowable();
            cause.readFields(in);
            ex.initCause(cause.ex);
        }

    }

    public Throwable getValue() {
        return ex;
    }

    @Override
    public String toString() {
        return ex.toString();
    }
}