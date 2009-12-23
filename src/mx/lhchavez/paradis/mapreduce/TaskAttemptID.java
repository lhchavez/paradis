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

package mx.lhchavez.paradis.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import mx.lhchavez.paradis.io.WritableComparable;
import mx.lhchavez.paradis.util.Progress;

/**
 *
 * @author lhchavez
 */
public class TaskAttemptID implements WritableComparable<TaskAttemptID> {
    private String jobID;
    private long taskID;
    private long attemptID;
    private long assignTime;
    private byte[] clientId = new byte[32];
    private Status status = Status.Unassigned;
    public static final int INDEX_LENGTH = 42;
    private Progress progress;

    public static enum Status {
        Unassigned,
        Assigned,
        Finished,
        Error
    };

    public TaskAttemptID(String jobID, long taskID, long attemptID) {
        this.jobID = jobID;
        this.taskID = taskID;
        this.attemptID = attemptID;
        this.assignTime = System.currentTimeMillis();
        this.status = Status.Unassigned;
        this.progress = new Progress();
        this.progress.setStatus(jobID + "/" + taskID);
    }

    public TaskAttemptID() {
    }

    /**
     * @return the jobID
     */
    public String getJobID() {
        return jobID;
    }

    /**
     * @return the taskID
     */
    public long getTaskID() {
        return taskID;
    }

    /**
     * @return the attemptID
     */
    public long getAttemptID() {
        return attemptID;
    }

    public void increaseAttempts() {
        this.attemptID++;
    }

    public long getAssignTime() {
        return this.assignTime;
    }

    public void touchAssignTime() {
        this.assignTime = System.currentTimeMillis();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(jobID);
        out.writeLong(taskID);
        out.writeByte((int)this.attemptID);
    }

    public void readFields(DataInput in) throws IOException {
        this.jobID = in.readUTF();
        this.taskID = in.readLong();
        this.attemptID = in.readByte();
    }

    public void writeIndex(DataOutput out) throws IOException {
        out.writeByte((int)this.attemptID);
        out.writeLong(assignTime);
        out.writeByte(status.ordinal());
        out.write(clientId);
    }

    public void readIndexFields(DataInput in) throws IOException {
        this.attemptID = in.readByte();
        this.assignTime = in.readLong();
        int ordinal = in.readByte();
        switch(ordinal) {
            case 0:
                status = Status.Unassigned;
                break;
            case 1:
                status = Status.Assigned;
                break;
            case 2:
                status = Status.Finished;
                break;
            case 3:
                status = Status.Error;
                break;
        }
        in.readFully(clientId);
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
    }

    /**
     * @param progress the progress to set
     */
    public void setProgress(Progress progress) {
        this.progress = progress;
    }

    /**
     * @return this task's progress
     */
    public Progress getProgress() {
        return progress;
    }

    @Override
    public String toString() {
        return "TaskAttemptID [job=" + jobID + ", task=" + taskID + ", attempt=" + attemptID + "]";
    }

    public int compareTo(TaskAttemptID o) {
        int ans = jobID.compareTo(o.jobID);

        if(ans != 0) return ans;

        if(taskID == o.taskID) return 0;
        if(taskID < o.taskID) return -1;
        return 1;
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof TaskAttemptID)) return false;

        TaskAttemptID taid = (TaskAttemptID)obj;

        return this.jobID.equals(taid.jobID) && this.taskID == taid.taskID;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 97 * hash + (this.jobID != null ? this.jobID.hashCode() : 0);
        hash = 97 * hash + (int) (this.taskID ^ (this.taskID >>> 32));
        return hash;
    }
}
