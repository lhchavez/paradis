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

package mx.lhchavez.paradis.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import mx.lhchavez.paradis.util.FileUtils;

/**
 *
 * @author lhchavez
 */
public class Interface {
    public static void usage() {
        System.err.println("paradis <command> [command parameters]");
        System.err.println();
        System.err.println("usage:");
        System.err.println();
        System.err.println("\tstandalone <file.zip>");
        System.err.println("\t\tRuns a job locally, with a standalone instance of paradis");
        System.err.println("\tenqueue <file.zip> <url>");
        System.err.println("\t\tSubmits a job to the paradis server");
        System.err.println("\tprogress <job id> <url>");
        System.err.println("\t\tGets the progress of a job");
        System.err.println("\toutput <job id> <url>");
        System.err.println("\t\tGets all of the output files of a job");
        System.err.println("\tversion <url>");
        System.err.println("\t\tGets the server version");
    }

    public static void main(String[] args) {
        if(args.length < 2) {
            usage();
            System.exit(1);
        }

        if(args[0].equals("standalone")) {
            File f = new File(args[1]);

            if(!f.exists()) {
                usage();
                System.exit(1);
            }

            try {
                Standalone.main(new String[] { f.getCanonicalPath() });
            } catch(Exception e) {
                System.err.println(e.getMessage());
                e.printStackTrace(System.err);
            }
        } else if(args[0].equals("enqueue")) {
            if(args.length < 3) {
                usage();
                System.exit(1);
            }

            File f = new File(args[1]);

            if(!f.exists()) {
                usage();
                System.exit(1);
            }

            try {
                URL root = new URL(args[2]);
                URL enqueue = new URL(root, "job/enqueue");

                HttpURLConnection conn = (HttpURLConnection)enqueue.openConnection();

                conn.setDoOutput(true);
                conn.setFixedLengthStreamingMode((int) f.length());

                OutputStream os = conn.getOutputStream();

                FileUtils.copy(new FileInputStream(f), os);

                os.close();

                int responseCode = conn.getResponseCode();
                if(responseCode == HttpURLConnection.HTTP_OK) {
                    System.out.print("Job Id: ");
                    FileUtils.copy(conn.getInputStream(), System.out, false);
                    System.out.println();
                } else if(responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
                    usage();
                    System.exit(1);
                } else {
                    FileUtils.copy(conn.getInputStream(), System.err, false);
                    System.out.println();
                    System.exit(1);
                }
            } catch(IOException e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        } else if(args[0].equals("progress")) {
            if(args.length < 3) {
                usage();
                System.exit(1);
            }

            try {
                URL root = new URL(args[2]);
                URL enqueue = new URL(root, "job/" + args[1] + "/progress");

                HttpURLConnection conn = (HttpURLConnection)enqueue.openConnection();

                int responseCode = conn.getResponseCode();
                if(responseCode == HttpURLConnection.HTTP_OK) {
                    FileUtils.copy(conn.getInputStream(), System.out, false);
                    System.out.println();
                } else if(responseCode == HttpURLConnection.HTTP_NOT_FOUND) {
                    System.err.println("Job " + args[1] + " not found!");
                    System.exit(1);
                } else {
                    FileUtils.copy(conn.getInputStream(), System.err, false);
                    System.out.println();
                    System.exit(1);
                }
            } catch(IOException e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        } else if(args[0].equals("output")) {
            if(args.length < 3) {
                usage();
                System.exit(1);
            }

            File outputDir = new File("output");
            outputDir.mkdir();

            try {
                URL root = new URL(args[2]);

                for(long l = 0; ; l++) {
                    URL download = new URL(root, String.format("job/%s/output/%08d.out", args[1], l));

                    FileUtils.download(download, new File(String.format("%s%s%08d.out", outputDir.getCanonicalPath(), File.separator, l)));
                }
            } catch(IOException e) {
            }
        } else if(args[0].equals("version")) {
            try {
                URL root = new URL(args[2]);
                URL enqueue = new URL(root, "version");

                HttpURLConnection conn = (HttpURLConnection)enqueue.openConnection();

                int responseCode = conn.getResponseCode();
                if(responseCode == HttpURLConnection.HTTP_OK) {
                    FileUtils.copy(conn.getInputStream(), System.out, false);
                    System.out.println();
                }
            } catch(IOException e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }
    }
}
