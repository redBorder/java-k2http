package net.redborder.k2http.thread;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * ThreadFactory implementation to give each Thread a name appended with a unique id. Also
 * defined priority.
 */
public class PriorityThreadFactory implements ThreadFactory {
    private final String name;
    /*
     * Thread Priority
     */
    private final int priority;
    /*
     * Whether to set newly created threads as daemons or not. Default false.
     */
    private boolean daemon;
    /**
     * Auto-Append id to each thread. Start from 1 not 0.
     */
    protected final AtomicInteger id = new AtomicInteger(1);

    public PriorityThreadFactory(String name) {
        this(name, Thread.NORM_PRIORITY, false);
    }

    public PriorityThreadFactory(String name, int priority) {
        this(name, priority, false);
    }

    public PriorityThreadFactory(String name, int priority, boolean daemon) {
        this.name = name;
        this.priority = priority;
        this.daemon = daemon;

    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, name + "-" + id.getAndIncrement());
        t.setPriority(priority);
        t.setDaemon(daemon);
        return t;
    }
}
