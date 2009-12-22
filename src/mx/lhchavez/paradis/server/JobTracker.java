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
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import mx.lhchavez.paradis.mapreduce.TaskAttemptID;
import mx.lhchavez.paradis.util.Configuration;

/**
 * @author lhchavez
 */
public class JobTracker implements Runnable {
    private TreeMap<String, Job> jobMap = new TreeMap<String, Job>();
    private LinkedBlockingQueue<Job> queueJobs = new LinkedBlockingQueue<Job>();
    private Job currentJob = null;
    private static JobTracker instance = null;
    private final Object jobMutex = new Object();

    private JobTracker() {
        File root = new File("jobs");
        root.mkdir();

        for(File jobDir : root.listFiles()) {
            if(!jobDir.isDirectory()) continue;

            try{
                if(new File(jobDir.getCanonicalPath() + File.separator + "finished").exists())
                    continue;

                File confFile = new File(jobDir.getCanonicalPath() + File.separator + "config.xml");
                if(confFile.exists()) {
                    Configuration conf = new Configuration(new FileInputStream(confFile), jobDir);

                    Job j = new Job(jobDir.getName(), jobDir, conf);
                    enqueue(j);
                }
            } catch(Exception e) {
                // don't even bother logging, we'll just hope the user remembers
                // to re-issue the job
            }
        }
    }

    public static synchronized JobTracker getInstance() {
        if (instance != null) {
            return instance;
        }
        
        instance = new JobTracker();
        
        new Thread(instance).start();

        return instance;
    }

    public Job enqueue(Job j) {
        queueJobs.add(j);
        j.setCallback(new JobFinishCallback() {
            public void JobFinished(Job j) {
                instance.jobEndSignal();
            }
        });
        jobMap.put(j.getID(), j);
        return j;
    }

    public Job dequeue() {
        try {
            return queueJobs.take();
        } catch (InterruptedException ex) {
            Logger.getLogger(JobTracker.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public Job getCurrentJob() {
        return currentJob;
    }

    public Job getById(String jobID) {
        return jobMap.get(jobID);
    }

    public void run() {
        while(true) {
            try {
                currentJob = queueJobs.take();
            } catch (InterruptedException ex) {
                continue;
            }

            currentJob.setStatus(Job.Status.Running);

            Logger.getLogger(JobTracker.class.getName()).log(Level.INFO, "Starting job " + currentJob.getID());
            
            try {
                currentJob.splitInput();
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(JobTracker.class.getName()).log(Level.SEVERE, null, ex);
            } catch (NoSuchMethodException ex) {
                Logger.getLogger(JobTracker.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InstantiationException ex) {
                Logger.getLogger(JobTracker.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IllegalAccessException ex) {
                Logger.getLogger(JobTracker.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IllegalArgumentException ex) {
                Logger.getLogger(JobTracker.class.getName()).log(Level.SEVERE, null, ex);
            } catch (InvocationTargetException ex) {
                Logger.getLogger(JobTracker.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(JobTracker.class.getName()).log(Level.SEVERE, null, ex);
            }

            Logger.getLogger(JobTracker.class.getName()).log(Level.INFO, "Job " + currentJob.getID() + " partitioned. Waiting for the mappers to finish.");

            synchronized(jobMutex) {
                try {
                    jobMutex.wait();
                } catch (InterruptedException ex) {
                    Logger.getLogger(JobTracker.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

            if(currentJob.getStatus() == Job.Status.Error) {
                Logger.getLogger(JobTracker.class.getName()).log(Level.SEVERE, "Job " + currentJob.getID() + " has errors.");
            } else {
                Logger.getLogger(JobTracker.class.getName()).log(Level.INFO, "Job " + currentJob.getID() + " reducing.");

                try {
                    currentJob.reduce();
                } catch (ClassNotFoundException ex) {
                    Logger.getLogger(JobTracker.class.getName()).log(Level.SEVERE, null, ex);
                } catch (NoSuchMethodException ex) {
                    Logger.getLogger(JobTracker.class.getName()).log(Level.SEVERE, null, ex);
                } catch (InstantiationException ex) {
                    Logger.getLogger(JobTracker.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IllegalAccessException ex) {
                    Logger.getLogger(JobTracker.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IllegalArgumentException ex) {
                    Logger.getLogger(JobTracker.class.getName()).log(Level.SEVERE, null, ex);
                } catch (InvocationTargetException ex) {
                    Logger.getLogger(JobTracker.class.getName()).log(Level.SEVERE, null, ex);
                } catch (IOException ex) {
                    Logger.getLogger(JobTracker.class.getName()).log(Level.SEVERE, null, ex);
                }
            }

            jobMap.remove(currentJob.getID());
            currentJob.cleanup();

            Logger.getLogger(JobTracker.class.getName()).log(Level.INFO, "Job " + currentJob.getID() + " finished.");
        }
    }

    void jobEndSignal() {
        synchronized(jobMutex) {
            jobMutex.notify();
        }
    }

    public synchronized TaskAttemptID getNextTask() throws IOException {
        TaskAttemptID taid = null;
        if(currentJob != null) {
            taid = currentJob.getNextTask();

            /*
            if(taid == null) {
                for(Job j : queueJobs) {
                    taid = currentJob.getNextTask();
                    if(taid != null) break;
                }
            }
            */
        }
        return taid;
    }
}
