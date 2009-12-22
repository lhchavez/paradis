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

import java.io.Closeable;
import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.Arrays;

/**
 *
 * @author lhchavez
 */
public class DataInputBuffer implements Closeable, DataInput {
    private byte[] data = new byte[0];
    private int position = 0;
    
    public void reset(byte[] input, int length) {
        reset(input, 0, length);
    }

    public void reset(byte[] input, int start, int length) {
        data = Arrays.copyOfRange(input, start, length);
    }

    public byte[] getData() {
        return data;
    }

    public int getPosition() {
        return position;
    }

    public int getLength() {
        return data.length;
    }
    
    public void close() throws IOException {
        data = new byte[0];
        position = 0;
    }

    public void readFully(byte[] b) throws IOException {
        if(b == null)
            throw new NullPointerException();

        for(int i = 0; i < b.length; i++)
            b[i] = readByte();
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        if(b == null)
            throw new NullPointerException();
        if( off < 0 || len < 0 || off + len > b.length)
            throw new IndexOutOfBoundsException();

        for(int i = 0; i < len; i++)
            b[off + i] = readByte();
    }

    public int skipBytes(int n) throws IOException {
        n = Math.min(n, data.length - position);
        position += n;

        return n;
    }

    public boolean readBoolean() throws IOException {
        return readByte() != 0;
    }

    public byte readByte() throws IOException {
        if(position == data.length)
            throw new EOFException();
        
        return data[position++];
    }

    public int readUnsignedByte() throws IOException {
        if(position == data.length)
            throw new EOFException();

        int ans = data[position] & 0x7f;
        if( (data[position] & 0x80) != 0 )
            ans |= 0x80;

        position++;
        
        return readByte() & 0xff;
    }

    public short readShort() throws IOException {
        byte a = readByte();
        byte b = readByte();

        return (short)((a << 8) | (b & 0xff));
    }

    public int readUnsignedShort() throws IOException {
        byte a = readByte();
        byte b = readByte();

        return (((a & 0xff) << 8) | (b & 0xff));
    }

    public char readChar() throws IOException {
        byte a = readByte();
        byte b = readByte();

        return (char)((a << 8) | (b & 0xff));
    }

    public int readInt() throws IOException {
        byte a = readByte();
        byte b = readByte();
        byte c = readByte();
        byte d = readByte();

        return (((a & 0xff) << 24) | ((b & 0xff) << 16) |
            ((c & 0xff) << 8) | (d & 0xff));
    }

    public long readLong() throws IOException {
        byte a = readByte();
        byte b = readByte();
        byte c = readByte();
        byte d = readByte();
        byte e = readByte();
        byte f = readByte();
        byte g = readByte();
        byte h = readByte();

        return (((long)(a & 0xff) << 56) |
          ((long)(b & 0xff) << 48) |
          ((long)(c & 0xff) << 40) |
          ((long)(d & 0xff) << 32) |
          ((long)(e & 0xff) << 24) |
          ((long)(f & 0xff) << 16) |
          ((long)(g & 0xff) <<  8) |
          ((long)(h & 0xff)));
    }

    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    public String readLine() throws IOException {
        if(position == data.length) return null;
        
        StringBuilder builder = new StringBuilder();
        char c;

        while(position != data.length) {
            c = (char)(data[position++] & 0xff);

            if(c == '\n') break;
            if(c != '\r') builder.append(c);
        }

        return builder.toString();
    }

    public String readUTF() throws IOException {
        short utfLength = readShort();
        byte a, b, c;

        if(utfLength + position > data.length)
            throw new EOFException();

        StringBuilder builder = new StringBuilder();

        while(utfLength > 0) {
            a = readByte();
            utfLength--;

            if( (a & 0x80) == 0x00 ) {
                builder.append((char)a);
            } else if( (a & 0xE0) == 0xC0 ) {
                if(utfLength < 1)
                    throw new UTFDataFormatException();

                b = readByte();
                utfLength--;

                if( (b & 0xC0) != 0x80 )
                    throw new UTFDataFormatException();

                builder.append( (char)(((a& 0x1F) << 6) | (b & 0x3F)) );
            } else if( (a & 0xF0 ) == 0xE0 ) {
                if(utfLength < 2)
                    throw new UTFDataFormatException();

                b = readByte(); c = readByte();
                utfLength -= 2;

                if( (b & 0xC0) != 0x80 || (c & 0xC0) != 0x80 )
                    throw new UTFDataFormatException();

                builder.append( (char)(((a & 0x0F) << 12) | ((b & 0x3F) << 6) | (c & 0x3F)) );
            }
        }

        return builder.toString();
    }
}
