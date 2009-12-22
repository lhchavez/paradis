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

package mx.lhchavez.paradis.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.URL;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 *
 * @author lhchavez
 */
public class FileUtils {

    public static String md5sum(InputStream file) {
        InputStream in = file;

        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[8192];
            int read = 0;

            while ((read = in.read(buffer)) > 0) {
                digest.update(buffer, 0, read);
            }

            byte[] md5sum = digest.digest();
            BigInteger bigInt = new BigInteger(1, md5sum);
            return bigInt.toString(16);
        } catch (Exception e) {
            throw new RuntimeException("Unable to process file for MD5", e);
        } finally {
            if(in != null)
            try {
                in.close();
            } catch (IOException e) {
                throw new RuntimeException("Unable to close input stream for MD5 calculation", e);
            }
        }
    }

    public static String sha1sum(InputStream file) {
        InputStream in = file;

        try {
            MessageDigest digest = MessageDigest.getInstance("SHA1");
            byte[] buffer = new byte[8192];
            int read = 0;

            while ((read = in.read(buffer)) > 0) {
                digest.update(buffer, 0, read);
            }

            byte[] sha1sum = digest.digest();
            BigInteger bigInt = new BigInteger(1, sha1sum);
            return bigInt.toString(16);
        } catch (Exception e) {
            throw new RuntimeException("Unable to process file for MD5", e);
        } finally {
            if(in != null)
            try {
                in.close();
            } catch (IOException e) {
                throw new RuntimeException("Unable to close input stream for MD5 calculation", e);
            }
        }
    }

    public static void extract(ZipFile zip, String filename, File directory) throws IOException {
        extract(zip, filename, directory, filename);
    }

    public static void extract(ZipFile zip, String filename, File directory, String outputFilename) throws IOException {
        byte[] buffer = new byte[8192];
        int read;

        ZipEntry entry = zip.getEntry(filename);
        File output = new File(directory.getCanonicalPath() + File.separator + outputFilename);
        output.getParentFile().mkdirs();
        FileOutputStream jarOStream = new FileOutputStream(output);
        InputStream jarIStream = zip.getInputStream(entry);

        while((read = jarIStream.read(buffer)) > 0) jarOStream.write(buffer, 0, read);

        jarOStream.close(); jarIStream.close();
    }

    public static void download(URL url, File destination) throws IOException {
        byte[] buffer = new byte[4096];
        int read, total = 0;

        URLConnection conn = url.openConnection();
        conn.connect();
        InputStream is = conn.getInputStream();
        FileOutputStream out = new FileOutputStream(destination);

        while((read = is.read(buffer)) > 0) {
            out.write(buffer, 0, read);
            total += read;
        }

        if(total < conn.getContentLength())
            throw new IOException("Download truncated");

        is.close();
        out.close();
    }

    public static void copy(InputStream is, OutputStream os) throws IOException {
        copy(is, os, true);
    }

    public static void copy(InputStream is, OutputStream os, boolean closeOutput) throws IOException {
        byte[] buffer = new byte[4096];
        int read;

        while((read = is.read(buffer)) > 0)
            os.write(buffer, 0, read);

        if(closeOutput)
            os.close();
        os.flush();
    }
}
