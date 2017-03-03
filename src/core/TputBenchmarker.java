package core;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import core.utils.Operation;
import core.utils.PriorityComparator;


public class TputBenchmarker {

    static AbstractQueue q;
    static ArrayList<Long> executionTimes;
    static ConcurrentLinkedQueue<Integer> completionTimes;

    static private String extrapolateStackTrace(Exception ex) {
        Throwable e = ex;
        String trace = e.toString() + "\n";
        for (StackTraceElement e1 : e.getStackTrace()) {
            trace += "\t at " + e1.toString() + "\n";
        }
        while (e.getCause() != null) {
            e = e.getCause();
            trace += "Cause by: " + e.toString() + "\n";
            for (StackTraceElement e1 : e.getStackTrace()) {
                trace += "\t at " + e1.toString() + "\n";
            }
        }
        return trace;
    }

    public static void main(String args[]) throws InterruptedException {

        executionTimes = new ArrayList<Long>();
        completionTimes = new ConcurrentLinkedQueue<>();
        int repeats = 50;

        for(int x=0; x<repeats; x++) {
            //q = new PriorityBlockingQueue<Operation>();
            //q = new ConcurrentSkipListPriorityQueue<Operation>(new PriorityComparator());
            q = new ConcurrentPriorityQueue<Operation>(new PriorityComparator());
            //q = new MultiConcurrentLinkedPriorityQueueDRR(Arrays.asList(2, 2, 2, 2, 2));
            //q = new MultiConcurrentLinkedPriorityQueue(Arrays.asList(2, 2, 2, 2, 2));
            //q = new ConcurrentLinkedQueue();

            int jobs = 32;
            final int seed = 444;
            Random r = new Random(seed);

            for (int i = 0; i < 10; i++) {
                q.offer(new Operation(r.nextInt(6)));
            }

            // create a pool of threads, 32 max jobs will execute in parallel
            ExecutorService threadPool = Executors.newFixedThreadPool(jobs);

            long startTime = System.currentTimeMillis();
            // submit jobs to be executing by the pool
            for (int i = 0; i < jobs; i++) {
                System.out.println("Submitting job:" + (i + 1));
                threadPool.submit(new Runnable() {
                    public void run() {
                        //code to run in parallel
                        Random rt = new Random();
                        int push = 0;
                        int pop = 0;
                        try {

                            for (int i = 0; i < 100000; i++) {
                                Thread t = Thread.currentThread();
                                String name = t.getName();
                                //System.out.println("name=" + name + " count=" + i + " QueueSize=" + q.size());
                                if (rt.nextBoolean()) {
                                    //push++;
                                    //q.add(rt.nextInt(20000));
                                    q.offer(new Operation(rt.nextInt(6)));
                                } else {
                                    //pop++;
                                    Operation o = (Operation)q.poll();
                                    //sample only 1% of the operations
                                    if(rt.nextInt(100)>=99)
                                        completionTimes.add((int)(System.currentTimeMillis()-o.getPriority().right));
                                }
                            }
                        } catch (Exception x) {
                            //System.out.println("Caught it" + extrapolateStackTrace(x));
                        }
                        Thread t = Thread.currentThread();
                        String name = t.getName();
                        //System.out.println("[FINISHED] name="+name+" adds="+(push-pop));
                    }
                });
            }

            // once you've submitted your last job to the service it should be shut down
            threadPool.shutdown();

            // wait for the threads to finish if necessary
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

            long endTime = System.currentTimeMillis();

            System.out.println("Queue " + q.toString());
            System.out.println("Queue Size: " + q.size());
            System.out.println("Total duration: " + (endTime - startTime));

            executionTimes.add((endTime - startTime));

        }

        System.out.println(executionTimes);

        try {
            FileWriter executionWriter = new FileWriter("executionTimes");
            FileWriter completionWriter = new FileWriter("completionTimes");

            for(int i=0; i<executionTimes.size();i++)
            executionWriter.append(Long.toString(executionTimes.get(i)))
                    .append(System.getProperty("line.separator"));

            for(int j=0; j<completionTimes.size();j++)
            completionWriter.append(Long.toString(completionTimes.poll()))
                    .append(System.getProperty("line.separator"));

        } catch (IOException e) {
        e.printStackTrace();
        }

    }

}
