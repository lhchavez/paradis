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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import mx.lhchavez.paradis.io.StreamRecordReader;
import mx.lhchavez.paradis.io.StreamRecordWriter;
import mx.lhchavez.paradis.io.WritableThrowable;
import mx.lhchavez.paradis.mapreduce.Mapper;
import mx.lhchavez.paradis.mapreduce.MapperContext;
import mx.lhchavez.paradis.mapreduce.TaskAttemptID;
import mx.lhchavez.paradis.util.Configuration;
import mx.lhchavez.paradis.util.FileUtils;

/**
 * @author lhchavez
 */
public class Client {
    private static String host = "paradis-dasl4ltd.qro.itesm.mx";
    private static int port = 17252;
    private static String uri = "/";
    private static URL paradis;
    public static boolean running = true;

    public static void main(String[] args) {
        if(args.length > 0) {
            try {
                 paradis = new URL(args[0]);
            } catch(MalformedURLException muex) {}
        } else {
            try {
                 paradis = new URL("http", host, port, uri);
            } catch(MalformedURLException muex) {}
        }

        Thread.currentThread().setPriority(Thread.MIN_PRIORITY);
        SecurityManager paradisSecManager = new ParadisSecurityManager(paradis.getHost(), paradis.getPort(), new File("").getAbsolutePath() + File.separator);
        System.setSecurityManager(paradisSecManager);
        File root = new File(".");
        
        while(running) {
            try {
                HttpURLConnection conn = (HttpURLConnection)new URL(paradis, "task/get").openConnection();

                if(conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                    DataInputStream in = new DataInputStream(conn.getInputStream());

                    in.readInt();

                    TaskAttemptID taid = new TaskAttemptID();

                    taid.readFields(in);

                    try{
                        File jobwd = new File("jobs" + File.separator + taid.getJobID());
                        File configXML = new File(jobwd.getCanonicalPath() + File.separator + "config.xml");

                        if(jobwd.mkdirs() || !configXML.exists()) {
                            // trabajo nuevo, hay que bajar muchas cosas del servidor

                            // config.xml
                            FileUtils.copy(new URL(paradis, "job/" + taid.getJobID() + "/config.xml").openStream(), new FileOutputStream(configXML));

                            // shared.zip
                            File sharedZip = new File(jobwd.getCanonicalPath() + File.separator + "shared.zip");
                            FileUtils.copy(new URL(paradis, "job/" + taid.getJobID() + "/shared.zip").openStream(), new FileOutputStream(sharedZip));

                            ZipInputStream zis = new ZipInputStream(new FileInputStream(jobwd.getCanonicalPath() + File.separator + "shared.zip"));
                            ZipEntry entry;
                            while((entry = zis.getNextEntry()) != null) {
                                File f = new File(jobwd.getCanonicalPath() + File.separator + entry.getName());
                                if(entry.isDirectory())
                                    f.mkdir();
                                else
                                    FileUtils.copy(zis, new FileOutputStream(f));
                            }

                            sharedZip.delete();
                        }

                        Configuration conf = new Configuration(new FileInputStream(configXML), jobwd);

                        for(String jarFile : conf.getStringArray("jar.file")) {
                            File f = new File(jobwd.getCanonicalPath() + File.separator + jarFile);
                            if(!f.exists()) {
                                FileUtils.copy(new URL(paradis, "job/" + taid.getJobID() + "/" + jarFile).openStream(), new FileOutputStream(f));
                            }
                        }

                        /*
                        for(String jarFile : conf.getStringArray("jar.libraries")) {
                            File f = new File("libraries" + File.separator + jarFile + ".jar");
                            if(!f.exists()) {
                                copy(new URL(paradis, "job/" + taid.getJobID() + "/" + jarFile).openStream(), f);
                            }
                        }
                        */

                        conf.validate(jobwd);

                        StreamRecordReader srr = new StreamRecordReader(in, conf.getKeyInClass(), conf.getValueInClass());
                        StreamRecordWriter srw = new StreamRecordWriter();

                        File outputFile = new File(jobwd.getCanonicalPath() + File.separator + taid.getTaskID());
                        srw.setOutput(new RandomAccessFile(outputFile, "rw"));

                        MapperContext context = new MapperContext(conf, taid, srr, srw);
                        Mapper m = conf.getMapperClass().newInstance();

                        m.run(context);

                        srw.close();

                        HttpURLConnection finishURL = (HttpURLConnection)new URL(paradis, "job/" + taid.getJobID() + "/task/" + taid.getTaskID() + "/finished").openConnection();

                        finishURL.setDoInput(false);
                        finishURL.setDoOutput(true);

                        finishURL.setFixedLengthStreamingMode((int) outputFile.length());
                        finishURL.addRequestProperty("Content-Type", "application/paradis-record");
                        OutputStream out = finishURL.getOutputStream();
                        FileInputStream fis = new FileInputStream(outputFile);
                        byte[] buffer = new byte[1024];
                        int read;

                        while((read = fis.read(buffer)) > 0) {
                            out.write(buffer, 0, read);
                        }
                        out.close();
                    } catch(Exception ex) {
                        WritableThrowable wt = new WritableThrowable(ex);

                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        DataOutputStream dos = new DataOutputStream(baos);
                        wt.write(dos);
                        dos.close();

                        HttpURLConnection finishURL = (HttpURLConnection)new URL(paradis, "job/" + taid.getJobID() + "/task/" + taid.getTaskID() + "/error").openConnection();

                        finishURL.setDoInput(false);
                        finishURL.setDoOutput(true);

                        finishURL.addRequestProperty("Content-Type", "application/paradis-record");
                        finishURL.setFixedLengthStreamingMode(baos.size());
                        OutputStream out = finishURL.getOutputStream();
                        byte[] buffer = new byte[1024];
                        int read;

                        out.write(baos.toByteArray());
                        out.close();
                    }
                } else if(conn.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                    try{
                        Thread.sleep(1000 * 60);
                    } catch(InterruptedException ex) {}
                } else {
                    System.err.println(conn.getContent());
                }
            } catch(Exception ex) {
                System.err.println(ex);
                ex.printStackTrace(System.err);
                
                try{
                    Thread.sleep(1000 * 60);
                } catch(InterruptedException iex) {}
            }
        }
    }
}
