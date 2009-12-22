/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package mx.lhchavez.paradis.client;

import org.tanukisoftware.wrapper.WrapperListener;
import org.tanukisoftware.wrapper.WrapperManager;

/**
 *
 * @author lhchavez
 */
public class Service implements WrapperListener, Runnable {
    private Thread clientThread;
    private String[] args;

    public Service() {
        args = new String[0];
    }
    
    public Service(String[] args) {
        this.args = args;
    }

    public void run() {
        Client.main(args);
    }

    public Integer start(String[] args) {
        clientThread = new Thread(this);
        clientThread.start();

        return null;
    }

    public int stop(int exitCode) {
        Client.running = false;
        clientThread.interrupt();

        return exitCode;
    }

    public void controlEvent(int event) {
        if( (event == WrapperManager.WRAPPER_CTRL_LOGOFF_EVENT) ) {
            // ignore :P
        } else {
            WrapperManager.stop(0);
        }
    }

    public static void main(String[] args) {
        WrapperManager.start(new Service(args), args);
    }
}
