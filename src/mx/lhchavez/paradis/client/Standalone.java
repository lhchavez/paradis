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
import java.io.RandomAccessFile;
import java.util.logging.Level;
import java.util.logging.Logger;
import mx.lhchavez.paradis.io.StreamRecordReader;
import mx.lhchavez.paradis.io.StreamRecordWriter;
import mx.lhchavez.paradis.mapreduce.Mapper;
import mx.lhchavez.paradis.mapreduce.MapperContext;
import mx.lhchavez.paradis.mapreduce.TaskAttemptID;
import mx.lhchavez.paradis.server.Job;
import mx.lhchavez.paradis.server.JobFactory;
import mx.lhchavez.paradis.util.Configuration;

/**
 *
 * @author lhchavez
 */
public class Standalone {
    public static void main(String[] args) throws Exception {
        Job currentJob = JobFactory.getInstance().createJob(args[0]);
        currentJob.setStatus(Job.Status.Running);

        Configuration conf = currentJob.getConfiguration();

        Logger.getLogger(Standalone.class.getName()).log(Level.INFO, "Starting job " + currentJob.getID());

        currentJob.splitInput();

        Logger.getLogger(Standalone.class.getName()).log(Level.INFO, "Job " + currentJob.getID() + " partitioned. Waiting for the mappers to finish.");
        
        Mapper mapper = conf.getMapperClass().newInstance();
        TaskAttemptID taid;

        File jobwd = new File("jobs" + File.separator + currentJob.getID());
        File inputDir = new File(jobwd.getCanonicalPath() + File.separator + "in");

        StreamRecordWriter srw = new StreamRecordWriter();

        while((taid = currentJob.getNextTask()) != null) {
            StreamRecordReader srr = new StreamRecordReader(new FileInputStream(inputDir.getCanonicalPath() + File.separator + taid.getTaskID()), conf.getKeyInClass(), conf.getValueInClass());
            File taskOutput = new File("tmp");
            srw.setOutput(new RandomAccessFile(taskOutput, "rw"));
            
            MapperContext mc = new MapperContext(currentJob.getConfiguration(), taid, srr, srw);
            mapper.run(mc);

            srw.close();

            currentJob.taskFinished(taid, new FileInputStream(taskOutput));

            taskOutput.delete();
        }

        if(currentJob.getStatus() == Job.Status.Error) {
            Logger.getLogger(Standalone.class.getName()).log(Level.SEVERE, "Job " + currentJob.getID() + " has errors.");
        } else {
            Logger.getLogger(Standalone.class.getName()).log(Level.INFO, "Job " + currentJob.getID() + " reducing.");

            currentJob.reduce();
        }

        currentJob.cleanup();

        Logger.getLogger(Standalone.class.getName()).log(Level.INFO, "Job " + currentJob.getID() + " finished.");
    }
}
