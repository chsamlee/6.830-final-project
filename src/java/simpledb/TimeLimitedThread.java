package simpledb;

/**
 * Example usage:
 *
 * Thread job = new Thread(() -> {...});
 * TimeLimitedThread th = new TimeLimitedThread(job, 1000);
 * th.start();
 * try {
 *     job.join();
 * }
 * catch (InterruptedException e) {
 *     // put timeout logic here
 * }
 * th.finish();
 */
public class TimeLimitedThread {

    private final Thread jobThread;
    private final Thread timerThread;
    private boolean finished;

    public TimeLimitedThread(final Thread job, final int limit) {
        this.jobThread = job;
        this.timerThread = new Thread(() -> {
            jobThread.start();
            try {
                Thread.sleep(limit);
            }
            catch (InterruptedException e) {
                finished = true;
            }
            if (!finished && jobThread.isAlive()) {
                jobThread.interrupt();
            }
        });
    }

    public void start() {
        timerThread.start();
    }

    public void finish() {
        finished = true;
    }

}
