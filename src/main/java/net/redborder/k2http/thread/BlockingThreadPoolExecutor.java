package net.redborder.k2http.thread;

import java.util.concurrent.*;
import org.slf4j.*;

public class BlockingThreadPoolExecutor extends ThreadPoolExecutor
{
    protected  final Logger logger = LoggerFactory.getLogger(BlockingThreadPoolExecutor.class);

    /**
     * RejectedExecutionHandler
     */
    private static final RejectedExecutionHandler rjeHandler = new RejectedExecutionHandler () {

        public void rejectedExecution(Runnable r, ThreadPoolExecutor tpe)
        {
            // Retry count for intrumentation and debugging
            int retryCount = 0;

            // Try indefinitely to add the task to the queue
            while (true)
            {
                retryCount++;

                if (tpe.isShutdown())  // If the executor is shutdown, reject the task and
                // throw RejectedExecutionException
                {
                    ((BlockingThreadPoolExecutor) tpe).taskRejectedGaveUp(r, retryCount);
                    throw new RejectedExecutionException("ThreadPool has been shutdown");
                }

                try
                {
                    if (tpe.getQueue().offer(r, 1, TimeUnit.SECONDS))
                    {
                        // Task got accepted!
                        ((BlockingThreadPoolExecutor) tpe).taskAccepted(r, retryCount);
                        break;
                    }
                    else
                        ((BlockingThreadPoolExecutor) tpe).taskRejectedRetrying(r, retryCount);

                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
            }
        }

    };


    /**
     * Default constructor. Create a ThreadPool of a single thread with a very large queue.
     */
    public BlockingThreadPoolExecutor()
    {
        this(1, 1, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(), new PriorityThreadFactory("workerthread"));
    }

    /**
     * Constructor
     * @param corePoolSize - Number of threads of keep in the pool
     * @param maxPoolSize - Maximum number of threads that can be spawned
     * @param queueCapacity - Size of the task queue
     * @param baseThreadName - Name assigned to each worker thread
     * @param priority - Priority of worker threads
     * @param daemon - If true, workers threads are created as daemons.
     *
     */
    public BlockingThreadPoolExecutor(int corePoolSize, int maxPoolSize, int queueCapacity, String baseThreadName, int priority, boolean daemon)
    {
        this(corePoolSize, maxPoolSize, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(queueCapacity), new PriorityThreadFactory(baseThreadName, priority, daemon));
    }

    /**
     * Constructor
     * @param corePoolSize - Number of threads of keep in the pool
     * @param maxPoolSize - Maximum number of threads that can be spawned
     * @param keepAliveTime
     * @param unit
     * @param queue
     * @param threadFactory
     */
    public BlockingThreadPoolExecutor(int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> queue, ThreadFactory threadFactory)
    {
        super(corePoolSize, maxPoolSize, keepAliveTime, unit, queue, threadFactory);
        allowCoreThreadTimeOut(true);

        // Attach a customer RejectedExecutionHandler defined above.
        this.setRejectedExecutionHandler(rjeHandler);
    }

    /**
     * Called when giving up on the task and rejecting it for good.
     * @param r
     * @param retryCount
     */
    protected void taskRejectedGaveUp(Runnable r, int retryCount)
    {
        //logger.debug("Gave Up: {}",retryCount );
    }

    /**
     * Called when the task that was rejected initially is rejected again.
     * @param r - Task
     * @param retryCount - number of total retries
     */
    protected void taskRejectedRetrying(Runnable r, int retryCount)
    {
        //logger.debug("Retrying: {}",retryCount );
    }

    /**
     * Called when the rejected task is finally accepted.
     * @param r - Task
     * @param retryCount - number of retries before acceptance
     */
    protected void taskAccepted(Runnable r, int retryCount)
    {
        //logger.debug("Accepted: {}",retryCount );
    }





}
