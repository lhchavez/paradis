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

import java.io.*;
import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import mx.lhchavez.paradis.io.Writable;
import mx.lhchavez.paradis.io.WritableComparable;
import mx.lhchavez.paradis.io.StreamRecordReader;
import mx.lhchavez.paradis.mapreduce.*;
import mx.lhchavez.paradis.util.Configuration;
import mx.lhchavez.paradis.util.FileUtils;
import mx.lhchavez.paradis.util.Progress;

/**
 *
 * @author lhchavez
 */
public class Job<KEYIN extends Writable, VALUEIN extends Writable, KEYOUT extends WritableComparable<KEYOUT>, VALUEOUT extends Writable, OUTKEY, OUTVAL> {

    private Configuration conf = null;
    private String jobID = null;
    private File jobDirectory;
    private File inputDirectory;
    private File outputDirectory;
    private File commitDirectory;
    private long splitCount;
    public static final long MAX_TASK_ATTEMPTS = 5;
    private Status status = Status.Pending;
    private InputFormat<KEYIN, VALUEIN> inputFormat;
    private OutputFormat<OUTKEY, OUTVAL> outputFormat;
    private Reducer<KEYOUT, VALUEOUT, OUTKEY, OUTVAL> reducer;
    private JobIndex index;
    private boolean splitted;
    private JobFinishCallback callback = null;

    public static enum Status {
        Pending,
        Running,
        Finished,
        Error
    };

    public Job(String jobID, File jobPath, Configuration conf) throws IOException {
        this.jobID = jobID;
        this.conf = conf;
        this.jobDirectory = jobPath;

        inputDirectory = new File(jobPath + File.separator + "in");
        inputDirectory.mkdir();
        outputDirectory = new File(jobPath + File.separator + "out");
        outputDirectory.mkdir();
        commitDirectory = new File(jobPath + File.separator + "output");
        commitDirectory.mkdir();
        new File(jobPath + File.separator + "errors").mkdir();

        index = new JobIndex(jobID, jobPath);
        splitted = index.recover();
    }

    public void splitInput() throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, IOException {
        if(!splitted) {
            InputFormatContext<KEYIN, VALUEIN> ctx = new InputFormatContext<KEYIN, VALUEIN>(conf, inputDirectory);

            inputFormat = conf.getInputFormatClass().newInstance();
            splitCount = inputFormat.getSplits(conf.getInt("splitCount", 100), ctx);

            index.build(splitCount);

            splitted = true;
        }
    }

    public void reduce() throws ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, IOException {
        outputFormat = conf.getOutputFormatClass().newInstance();
        
        ReducerContext ctx = new ReducerContext(this.getConfiguration(), this.getID(), outputFormat);

        TreeMap<KEYOUT, ArrayList<VALUEOUT>> outputMap = new TreeMap<KEYOUT, ArrayList<VALUEOUT>>();

        for(long l = 0; l < splitCount; l++) {
            StreamRecordReader<KEYOUT, VALUEOUT> resultReader = new StreamRecordReader<KEYOUT, VALUEOUT>(new FileInputStream(outputDirectory.getCanonicalPath() + File.separator + l), (Class<? extends KEYOUT>)conf.getKeyOutClass(), (Class<? extends VALUEOUT>)conf.getValueOutClass());
            while(resultReader.nextKeyValue()) {
                if (!outputMap.containsKey(resultReader.getCurrentKey())) {
                    outputMap.put(resultReader.getCurrentKey(), new ArrayList<VALUEOUT>());
                }

                outputMap.get(resultReader.getCurrentKey()).add(resultReader.getCurrentValue());
            }
            resultReader.close();
        }

        outputFormat.setOutputDirectory(commitDirectory);
        reducer = conf.getReducerClass().newInstance();

        for (Entry<KEYOUT, ArrayList<VALUEOUT>> e : outputMap.entrySet()) {
            try {
                reducer.reduce(e.getKey(), e.getValue(), ctx);
            } catch (InterruptedException ex) {
                Logger.getLogger(Job.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public Configuration getConfiguration() {
        return conf;
    }

    public TaskAttemptID getNextTask() throws IOException {
        return index.getNextTask();
    }

    public String getID() {
        return jobID;
    }

    public void taskFinished(TaskAttemptID taid, InputStream taskOutput) throws IOException {
        if(!taid.getJobID().equals(getID())) return;

        if (!index.setTaskFinished(taid.getTaskID())) return;

        Logger.getLogger(Job.class.getName()).log(Level.INFO, "Task " + taid.toString() + " finished.");

        FileUtils.copy(taskOutput, new FileOutputStream(outputDirectory.getCanonicalPath() + File.separator + taid.getTaskID()));

        if(index.isJobFinished()) {
            setStatus(Status.Finished);
            if(callback != null)
                callback.JobFinished(this);
        }
    }

    public void taskFinishedWithErrors(TaskAttemptID taid, Throwable ex) {
        if(!taid.getJobID().equals(getID())) return;

        if(index.setTaskError(taid.getTaskID())) {
            Logger.getLogger(Job.class.getName()).log(Level.SEVERE, "Task " + taid.toString() + " finished with errors.");
        } else {
            // let's help the user debug the problem by storing the exception
            setStatus(Status.Error);
            PrintWriter out = null;

            try {
                out = new PrintWriter(new FileWriter(jobDirectory.getCanonicalPath() + File.separator + "errors" + File.separator + taid.getTaskID() + "." + taid.getAttemptID()));
                out.println(ex.getMessage());
                ex.printStackTrace(out);
            } catch (IOException exc) {
                Logger.getLogger(Job.class.getName()).log(Level.SEVERE, "Error", exc);
            } finally {
                if(out != null)
                    out.close();
            }
            
            if(index.isJobFinished() && callback != null) {
                callback.JobFinished(this);
            }
        }
    }

    /**
     * @return the status
     */
    public Status getStatus() {
        return status;
    }

    /**
     * @param status the status to set
     */
    public void setStatus(Status status) {
        this.status = status;

        if(status == Status.Finished || status == Status.Error) {
            try {
                new FileOutputStream(jobDirectory.getCanonicalFile() + File.separator + "finished").close();
            } catch (IOException ex) {
                Logger.getLogger(Job.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public void cleanup() {
        if(status != Status.Finished) return;

        for(File f : jobDirectory.listFiles()) {
            if(f.getName().equals("output")) continue;
            if(f.getName().equals("finished")) continue;
            if(f.getName().equals("errors")) {
                // only delete errors directory if it's empty
                f.delete();
                continue;
            }

            if(f.isFile()) {
                f.delete();
            } else if(f.isDirectory()) {
                for(File child : f.listFiles()) {
                    child.delete();
                }
                f.delete();
            }
        }
    }

    void setCallback(JobFinishCallback jobFinishCallback) {
        callback = jobFinishCallback;
    }

    public Progress getProgress() {
        return index.getProgress();
    }
}
