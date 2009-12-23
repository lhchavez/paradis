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
package mx.lhchavez.paradis.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.zip.*;
import mx.lhchavez.paradis.util.Configuration;
import mx.lhchavez.paradis.util.FileUtils;

/**
 *
 * @author lhchavez
 */
public class JobFactory {

    private static JobFactory instance;

    static {
        instance = new JobFactory();
    }

    private JobFactory() {
    }

    public static JobFactory getInstance() {
        return instance;
    }

    public Job createJob(String zipFile) throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException {
        ZipFile zip = new ZipFile(zipFile);
        InputStream is = zip.getInputStream(zip.getEntry("config.xml"));
        Configuration conf = new Configuration(is);

        ArrayList<String> jarFiles = new ArrayList<String>();
        for(String jarFile : conf.getStringArray("jar.file")) {
            ZipEntry jarEntry = zip.getEntry(jarFile);

            // the zipFile must contain every single .jar described
            if(jarEntry == null) {
                throw new IllegalArgumentException("The input zipfile is missing the jar file " + jarFile);
            }
            jarFiles.add(jarFile);
        }

        String jobID = "";

        if(conf.getString("name") != null)
            jobID = conf.getString("name") + "_";

        jobID += System.currentTimeMillis();
        
        File jobwd = new File("jobs" + File.separator + jobID);
        jobwd.mkdirs();

        conf.setJobDirectory(jobwd);

        // let's copy the jar files and the config.xml to the working directory

        for(String jarFile : jarFiles) {
            FileUtils.extract(zip, jarFile, jobwd, jarFile);
        }

        conf.validate(jobwd);

        Enumeration<? extends ZipEntry> entries = zip.entries();

        new File(jobID + File.separator + "errors").mkdir();

        // extract any data to the filesystem
        File dataOutput = new File(jobID + File.separator + "data");
        dataOutput.mkdir();

        while(entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();

            String filename = entry.getName();

            if(!filename.startsWith("data/") || filename.equals("data/")) continue;

            File outputFile = new File(jobwd.getCanonicalPath() + File.separator + filename);

            if(filename.endsWith("/"))
                outputFile.mkdirs();
            else {
                FileUtils.extract(zip, filename, jobwd);
            }
        }

        // also the shared files. plus, re-compress them into shared.zip
        File sharedOutput = new File(jobID + File.separator + "shared");
        sharedOutput.mkdir();

        entries = zip.entries();
        ZipOutputStream sharedZip = new ZipOutputStream(new FileOutputStream(jobwd.getCanonicalPath() + File.separator + "shared.zip"));
        sharedZip.setLevel(9);
        sharedZip.putNextEntry(new ZipEntry("shared/"));
        while(entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();

            String filename = entry.getName();

            if(!filename.startsWith("shared/") || filename.equals("shared/")) continue;

            File outputFile = new File(jobwd.getCanonicalPath() + File.separator + filename);

            if(filename.endsWith("/"))
                outputFile.mkdirs();
            else {
                FileUtils.extract(zip, filename, jobwd);
            }

            sharedZip.putNextEntry(new ZipEntry(entry.getName()));
            FileUtils.copy(new FileInputStream(outputFile), sharedZip, false);
            sharedZip.closeEntry();
        }
        sharedZip.close();

        FileUtils.extract(zip, "config.xml", jobwd);

        zip.close();

        Job j = new Job(jobID, jobwd, conf);
        return j;
    }

}
