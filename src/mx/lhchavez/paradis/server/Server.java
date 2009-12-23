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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import mx.lhchavez.paradis.io.WritableThrowable;
import mx.lhchavez.paradis.mapreduce.TaskAttemptID;
import mx.lhchavez.paradis.util.Progress;
import org.restlet.Application;
import org.restlet.Component;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.Restlet;
import org.restlet.data.MediaType;
import org.restlet.data.Protocol;
import org.restlet.data.Status;
import org.restlet.representation.FileRepresentation;
import org.restlet.representation.StringRepresentation;
import org.restlet.routing.Router;

/**
 *
 * @author lhchavez
 */
public class Server extends Application {

    public static void main(String[] args) {
        try {
            // Create a new Component.
            Component component = new Component();

            // Add a new HTTP server listening on port 17252.
            component.getServers().add(Protocol.HTTP, 17252);

            // Attach the sample application.
            component.getDefaultHost().attach(new Server());

            // Start the component.
            component.start();
        } catch (Exception e) {
            // Something is wrong.
            e.printStackTrace();
        }
    }

    /**
     * Creates a root Restlet that will receive all incoming calls.
     */
    @Override
    public synchronized Restlet createInboundRoot() {
        Router router = new Router(getContext());

        router.attach("/job/enqueue", new Restlet(getContext()) {
            @Override
            public void handle(Request request, Response response) {
                JobFactory factory = JobFactory.getInstance();
                JobTracker tracker = JobTracker.getInstance();

                File zipFile = null;
                
                try {
                    zipFile = File.createTempFile("paradis", ".zip");
                    FileOutputStream fos = new FileOutputStream(zipFile);

                    byte[] buffer = new byte[4096];
                    int read;

                    InputStream is = request.getEntity().getStream();

                    while((read = is.read(buffer)) > 0) fos.write(buffer, 0, read);

                    is.close();
                    fos.close();

                    Job j = factory.createJob(zipFile.getCanonicalPath());

                    response.setEntity(new StringRepresentation(j.getID(), MediaType.TEXT_PLAIN));

                    tracker.enqueue(j);
                } catch (Exception ex) {
                    response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
                    response.setEntity(new StringRepresentation(exceptionToString(ex), MediaType.TEXT_PLAIN));
                } finally {
                    if(zipFile != null) {
                        zipFile.delete();
                    }
                }
            }
        });

        router.attach("/job/{jobId}/output/{file}", new Restlet(getContext()) {
            @Override
            public void handle(Request request, Response response) {

                File f = new File("jobs" + File.separator + request.getAttributes().get("jobId") + File.separator + "output" + File.separator + request.getAttributes().get("file"));

                if(f.exists()) {
                    response.setEntity(new FileRepresentation(f, MediaType.APPLICATION_OCTET_STREAM));
                    return;
                }

                response.setStatus(Status.CLIENT_ERROR_NOT_FOUND);
                response.setEntity(new StringRepresentation("", MediaType.TEXT_PLAIN));
            }
        });

        router.attach("/job/{jobId}/{jarFile}.jar", new Restlet(getContext()) {
            @Override
            public void handle(Request request, Response response) {
                Job j = JobTracker.getInstance().getById((String)request.getAttributes().get("jobId"));

                if(j != null) {
                    File f = new File("jobs" + File.separator + j.getID() + File.separator + request.getAttributes().get("jarFile") + ".jar");

                    if(f.exists()) {
                        response.setEntity(new FileRepresentation(f, MediaType.APPLICATION_JAVA_ARCHIVE));
                        return;
                    }
                }
                response.setStatus(Status.CLIENT_ERROR_NOT_FOUND);
                response.setEntity(new StringRepresentation("", MediaType.TEXT_PLAIN));
            }
        });

        router.attach("/job/{jobId}/shared.zip", new Restlet(getContext()) {
            @Override
            public void handle(Request request, Response response) {
                Job j = JobTracker.getInstance().getById((String)request.getAttributes().get("jobId"));

                if(j != null) {
                    File f = new File("jobs" + File.separator + j.getID() + File.separator + "shared.zip");

                    if(f.exists()) {
                        response.setEntity(new FileRepresentation(f, MediaType.APPLICATION_ZIP));
                        return;
                    }
                }
                response.setStatus(Status.CLIENT_ERROR_NOT_FOUND);
                response.setEntity(new StringRepresentation("", MediaType.TEXT_PLAIN));
            }
        });

        router.attach("/job/{jobId}/config.xml", new Restlet(getContext()) {
            @Override
            public void handle(Request request, Response response) {
                Job j = JobTracker.getInstance().getById((String)request.getAttributes().get("jobId"));

                if(j != null) {
                    File f = new File("jobs" + File.separator + j.getID() + File.separator + "config.xml");

                    if(f.exists()) {
                        response.setEntity(new FileRepresentation(f, MediaType.TEXT_XML));
                        return;
                    }
                }
                response.setStatus(Status.CLIENT_ERROR_NOT_FOUND);
                response.setEntity(new StringRepresentation("", MediaType.TEXT_PLAIN));
            }
        });

        router.attach("/job/{jobId}/task/get", new Restlet(getContext()) {
            @Override
            public void handle(Request request, Response response) {
                Job j = JobTracker.getInstance().getById((String)request.getAttributes().get("jobId"));

                if(j != null) {
                    try {
                        TaskAttemptID taid = j.getNextTask();

                        if(taid == null) {
                            response.setStatus(Status.CLIENT_ERROR_NOT_FOUND);
                            response.setEntity(new StringRepresentation("", MediaType.TEXT_PLAIN));
                        } else {
                            response.setEntity(new TaskRepresentation(taid));
                        }
                    } catch (IOException ex) {
                        response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
                        response.setEntity(new StringRepresentation(exceptionToString(ex), MediaType.TEXT_PLAIN));
                    }
                } else {
                    response.setStatus(Status.CLIENT_ERROR_NOT_FOUND);
                    response.setEntity(new StringRepresentation("", MediaType.TEXT_PLAIN));
                }
            }
        });

        router.attach("/job/{jobId}/task/{taskId}/get", new Restlet(getContext()) {
            @Override
            public void handle(Request request, Response response) {
                Job j = JobTracker.getInstance().getById((String)request.getAttributes().get("jobId"));

                if(j != null) {
                    TaskAttemptID taid = new TaskAttemptID(j.getID(), Long.valueOf((String)request.getAttributes().get("taskId")), 0);
                    
                    try {
                        response.setEntity(new TaskRepresentation(taid));
                    } catch (IOException ex) {
                        response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
                        response.setEntity(new StringRepresentation(exceptionToString(ex), MediaType.TEXT_PLAIN));
                    }
                } else {
                    response.setStatus(Status.CLIENT_ERROR_NOT_FOUND);
                    response.setEntity(new StringRepresentation("", MediaType.TEXT_PLAIN));
                }
            }
        });

        router.attach("/job/{jobId}/task/{taskId}/finished", new Restlet(getContext()) {
            @Override
            public void handle(Request request, Response response) {
                Job j = JobTracker.getInstance().getById((String)request.getAttributes().get("jobId"));

                if(j != null) {
                    TaskAttemptID taid = new TaskAttemptID(j.getID(), Long.valueOf((String)request.getAttributes().get("taskId")), 0);

                    try {
                        j.taskFinished(taid, request.getEntity().getStream());

                        response.setEntity(new StringRepresentation("", MediaType.TEXT_PLAIN));
                    } catch (IOException ex) {
                        response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
                        response.setEntity(new StringRepresentation(exceptionToString(ex), MediaType.TEXT_PLAIN));
                    }
                } else {
                    response.setStatus(Status.CLIENT_ERROR_NOT_FOUND);
                    response.setEntity(new StringRepresentation("", MediaType.TEXT_PLAIN));
                }
            }
        });

        router.attach("/job/{jobId}/task/{taskId}/error", new Restlet(getContext()) {
            @Override
            public void handle(Request request, Response response) {
                Job j = JobTracker.getInstance().getById((String)request.getAttributes().get("jobId"));

                if(j != null) {
                    TaskAttemptID taid = new TaskAttemptID(j.getID(), Long.valueOf((String)request.getAttributes().get("taskId")), 0);

                    try {
                        WritableThrowable throwable = new WritableThrowable();

                        DataInputStream dis = new DataInputStream(request.getEntity().getStream());

                        throwable.readFields(dis);

                        dis.close();

                        j.taskFinishedWithErrors(taid, throwable.getValue());

                        response.setEntity(new StringRepresentation("", MediaType.TEXT_PLAIN));
                    } catch (IOException ex) {
                        response.setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
                        response.setEntity(new StringRepresentation(exceptionToString(ex), MediaType.TEXT_PLAIN));
                    }
                } else {
                    response.setStatus(Status.CLIENT_ERROR_NOT_FOUND);
                    response.setEntity(new StringRepresentation("", MediaType.TEXT_PLAIN));
                }
            }
        });

        router.attach("/job/{jobId}/task/{taskId}/progress", new Restlet(getContext()) {
            @Override
            public void handle(Request request, Response response) {
                Job j = JobTracker.getInstance().getById((String)request.getAttributes().get("jobId"));

                if(j != null) {
                    TaskAttemptID taid = new TaskAttemptID(j.getID(), Long.valueOf((String)request.getAttributes().get("taskId")), 0);
                    taid.touchAssignTime();
                    Progress p = taid.getProgress();
                    if(p != null)
                        p.set(Float.valueOf(request.getEntityAsText()));
                    response.setEntity(new StringRepresentation("", MediaType.TEXT_PLAIN));
                } else {
                    response.setStatus(Status.CLIENT_ERROR_NOT_FOUND);
                    response.setEntity(new StringRepresentation("", MediaType.TEXT_PLAIN));
                }
            }
        });

        router.attach("/job/{jobId}/progress", new Restlet(getContext()) {
            @Override
            public void handle(Request request, Response response) {
                File finished = new File("jobs" + File.separator + (String)request.getAttributes().get("jobId") + File.separator + "finished");

                if(finished.exists()) {
                    response.setEntity(new StringRepresentation(String.format("{\"status\":\"%s\",\"progress\":1.0,\"total\":1.0}", (String)request.getAttributes().get("jobId")), MediaType.TEXT_PLAIN));
                } else {
                    Job j = JobTracker.getInstance().getById((String)request.getAttributes().get("jobId"));

                    if(j != null) {
                        response.setEntity(new StringRepresentation(j.getProgress().toString(), MediaType.TEXT_PLAIN));
                    } else {
                        response.setStatus(Status.CLIENT_ERROR_NOT_FOUND);
                        response.setEntity(new StringRepresentation("", MediaType.TEXT_PLAIN));
                    }
                }
            }
        });

        router.attach("/task/get", new Restlet(getContext()) {
            @Override
            public void handle(Request request, Response response) {
                TaskAttemptID taid;

                try {
                    taid = JobTracker.getInstance().getNextTask();
                } catch (IOException ex) {
                    response.setStatus(Status.SERVER_ERROR_INTERNAL);
                    response.setEntity(new StringRepresentation(exceptionToString(ex), MediaType.TEXT_PLAIN));
                    return;
                }

                if(taid != null) {
                    try {
                        response.setEntity(new TaskRepresentation(taid));
                    } catch (IOException ex) {
                        response.setStatus(Status.SERVER_ERROR_INTERNAL);
                        response.setEntity(new StringRepresentation(exceptionToString(ex), MediaType.TEXT_PLAIN));
                    }
                } else {
                    response.setStatus(Status.CLIENT_ERROR_NOT_FOUND);
                    response.setEntity(new StringRepresentation("", MediaType.TEXT_PLAIN));
                }
            }
        });

        router.attach("/version", new Restlet(getContext()) {
            @Override
            public void handle(Request request, Response response) {
                response.setEntity("1.0b", MediaType.TEXT_PLAIN);
            }
        });
        
        return router;
    }

    private static String exceptionToString(Throwable ex) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintWriter out = new PrintWriter(new OutputStreamWriter(baos));
        out.print(ex.getMessage());
        ex.printStackTrace(out);
        out.close();

        return baos.toString();
    }
}
