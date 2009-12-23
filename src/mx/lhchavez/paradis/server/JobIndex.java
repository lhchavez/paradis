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

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import mx.lhchavez.paradis.mapreduce.TaskAttemptID;
import mx.lhchavez.paradis.util.Progress;

/**
 *
 * @author lhchavez
 */
public class JobIndex implements Closeable {
    private Queue<TaskAttemptID> pendingTasks;
    private Map<Long, TaskAttemptID> waitingTasks;
    private RandomAccessFile indexFile;
    private String jobId;
    private Progress progress;
    private Progress mapperProgress;
    private Progress reducerProgress;

    public JobIndex(String jobId, File directory) throws IOException {
        this.jobId = jobId;
        pendingTasks = new LinkedList<TaskAttemptID>();
        waitingTasks = new TreeMap<Long, TaskAttemptID>();

        progress = new Progress();
        progress.setStatus(jobId);

        mapperProgress = progress.addPhase("mapPhase");
        reducerProgress = progress.addPhase("reducePhase");

        indexFile = new RandomAccessFile(directory.getCanonicalPath() + File.separator + "index", "rw");
    }

    public boolean recover() throws IOException {
        if(indexFile.length() > 0) {
            try {
                while(true) {
                    TaskAttemptID taid = new TaskAttemptID();
                    taid.readIndexFields(indexFile);
                    taid.setProgress(mapperProgress.addPhase(String.valueOf(taid.getTaskID())));

                    if(taid.getStatus() != TaskAttemptID.Status.Finished) {
                        pendingTasks.add(taid);
                    } else {
                        taid.getProgress().set(1.0f);
                    }
                }
            } catch(EOFException ex) {
                // ignore
            }

            return true;
        }

        return false;
    }

    public void build(long taskCount) throws IOException {
        TaskAttemptID taid;

        for (long i = 0; i < taskCount; i++) {
            taid = new TaskAttemptID(this.jobId, i, 0);
            taid.setProgress(mapperProgress.addPhase(String.valueOf(i)));
            pendingTasks.add(taid);
            taid.writeIndex(indexFile);
        }
    }

    public synchronized TaskAttemptID getNextTask() {
        if(pendingTasks.size() > 0) {
            TaskAttemptID taid = pendingTasks.remove();
            taid.increaseAttempts();
            taid.touchAssignTime();
            taid.setStatus(TaskAttemptID.Status.Assigned);
            waitingTasks.put(taid.getTaskID(), taid);

            try {
                indexFile.seek(taid.getTaskID() * TaskAttemptID.INDEX_LENGTH);
                taid.writeIndex(indexFile);
            } catch(IOException ex) {}

            return taid;
        } else {
            long currentTime = System.currentTimeMillis();
            for(TaskAttemptID taid : waitingTasks.values()) {
                if(currentTime - taid.getAssignTime() > 5 * 60 * 1000) {
                    // task has been assigned for more than 5 minutes without response
                    taid.increaseAttempts();
                    taid.touchAssignTime();
                    
                    try {
                        indexFile.seek(taid.getTaskID() * TaskAttemptID.INDEX_LENGTH);
                        taid.writeIndex(indexFile);
                    } catch(IOException ex) {}

                    return taid;
                }
            }
        }

        return null;
    }

    public synchronized boolean setTaskFinished(long taskID) {
        TaskAttemptID taid = waitingTasks.get(taskID);

        if(taid == null) return false;

        taid.setStatus(TaskAttemptID.Status.Finished);
        Progress p = taid.getProgress();
        if(p != null)
            p.set(1.0f);

        waitingTasks.remove(taskID);

        try {
            indexFile.seek(taid.getTaskID() * TaskAttemptID.INDEX_LENGTH);
            taid.writeIndex(indexFile);
        } catch(IOException ex) {}

        return true;
    }

    public synchronized boolean isJobFinished() {
        return pendingTasks.size() == 0 && waitingTasks.size() == 0;
    }

    public synchronized boolean setTaskError(long taskID) {
        TaskAttemptID taid = waitingTasks.get(taskID);

        boolean isFatal = false;

        if(taid == null) return false;
        Progress p = taid.getProgress();
        if(p != null)
            p.set(0);

        if(taid.getAttemptID() == Job.MAX_TASK_ATTEMPTS) {
            isFatal = true;
            taid.setStatus(TaskAttemptID.Status.Error);
        } else {
            taid.setStatus(TaskAttemptID.Status.Unassigned);
            pendingTasks.add(taid);
        }
        waitingTasks.remove(taskID);

        try {
            indexFile.seek(taid.getTaskID() * TaskAttemptID.INDEX_LENGTH);
            taid.writeIndex(indexFile);
        } catch(IOException ex) {}

        return isFatal;
    }

    public void close() throws IOException {
        indexFile.close();
    }

    public Progress getProgress() {
        return progress;
    }

    public Progress getReducerProgress() {
        return reducerProgress;
    }
}
