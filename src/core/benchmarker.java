package core;

import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class benchmarker {

    //static PriorityQueue q = new PriorityQueue();
    //static ConcurrentSkipListPriorityQueue q = new ConcurrentSkipListPriorityQueue();
    static ConcurrentLinkedQueue q = new ConcurrentLinkedQueue();

    public static void main(String args[]) throws InterruptedException {
        int jobs = 32;
        final int seed = 444;
        Random r = new Random(seed);

        for (int i=0;i<10000;i++)
        {
            q.add(r.nextInt(20000));
        }

        // create a pool of threads, 32 max jobs will execute in parallel
        ExecutorService threadPool = Executors.newFixedThreadPool(jobs);

        long startTime = System.currentTimeMillis();
        // submit jobs to be executing by the pool
        for (int i = 0; i < jobs; i++) {
            System.out.println("Submitting job:" + (i+1));
            threadPool.submit(new Runnable() {
                public void run() {
                    //code to run in parallel
                    Random rt = new Random();
                    int push = 0;
                    int pop = 0;

                    for(int i=0;i<100000;i++)
                    {
                        Thread t = Thread.currentThread();
                        String name = t.getName();
                        //System.out.println("name=" + name + " count=" + i + " QueueSize=" + q.size());
                        if(rt.nextBoolean()) {
                            //push++;
                            //q.add(rt.nextInt(20000));
                            q.offer(rt.nextInt(20000));
                        }
                        else {
                            //pop++;
                            q.poll();
                        }
                    }
                    Thread t = Thread.currentThread();
                    String name = t.getName();
                    //System.out.println("Queue " + q.toString());
                    //System.out.println("[FINISHED] name="+name+" adds="+(push-pop));
                }
            });
        }

        // once you've submitted your last job to the service it should be shut down
        threadPool.shutdown();

        // wait for the threads to finish if necessary
        threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

        long endTime = System.currentTimeMillis();

        System.out.println("Queue Size: "+ q.size());
        System.out.println("Total duration: " + (endTime - startTime));

    }
}
