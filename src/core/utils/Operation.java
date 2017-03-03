package core.utils;

import core.ConcurrentPriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by reda on 12/02/17.
 */
public class Operation implements PriorityProvider, Comparable<Operation>, Runnable {

    int server;
    int id;
    PriorityTuple priority;
    int serviceTime;
    ConcurrentHashMap<String, FileWriter> filePointers;
    private static final Logger logger = LoggerFactory.getLogger(Operation.class);

    public Operation(double p)
    {
        id = 0;
        server = 0;
        priority = new PriorityTuple(p, System.currentTimeMillis());
        serviceTime = 10;
    }

    public Operation(double p, int st)
    {
        id = 0;
        server = 0;
        priority = new PriorityTuple(p, System.currentTimeMillis());
        serviceTime = st;
    }

    public Operation(int i, double p, int st)
    {
        id = i;
        server = 0;
        priority = new PriorityTuple(p, System.currentTimeMillis());
        serviceTime = st;
    }

    public Operation(int i, int s, double p, int st)
    {
        id = i;
        server = s;
        priority = new PriorityTuple(p, System.currentTimeMillis());
        serviceTime = st;
    }

    public Operation(int i, int s, double p, int st, ConcurrentHashMap<String, FileWriter> fp)
    {
        id = i;
        server = s;
        priority = new PriorityTuple(p, System.currentTimeMillis());
        serviceTime = st;
        filePointers = fp;
    }

    @Override
    public PriorityTuple getPriority() {
        return priority;
    }

    @Override
    public int getBatchSize() {
        return 0;
    }

    @Override
    public int compareTo(Operation o) {
        return ((PriorityProvider)this).getPriority().compareTo(((PriorityProvider)o).getPriority());
    }

    public String toString() {
        return "Priority: " + priority.left;
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p/>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        long start = System.nanoTime();

        //create temp log file for each thread using this class
        try {
            String name = "temp/latencies" + Thread.currentThread().getId() + Thread.currentThread().getName();
            FileWriter latencyWriter;
            if(filePointers.containsKey(name))
                latencyWriter = filePointers.get(name);
            else
            {
                latencyWriter = new FileWriter(new File(name), true);
                filePointers.put(name, latencyWriter);
            }

            //logger.info("Before: {}", System.currentTimeMillis());
            while ((System.nanoTime() - start) < serviceTime*1000000) {
                //Do nothing
            }
            //logger.info("After: {}", System.currentTimeMillis());

            appendToFile(latencyWriter, System.currentTimeMillis() - priority.right);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void appendToFile(FileWriter writer, long latency)
    {
        //System.out.println("Appending to file latency" + Thread.currentThread().toString());
        String str = Integer.toString((int)(latency));
        try {
            writer.append(Long.toString(System.currentTimeMillis()))
                    .append(',')
                    .append(Integer.toString(id))
                    .append(',')
                    .append(Integer.toString(server))
                    .append(',')
                    .append(Long.toString(latency))
                    .append(System.getProperty("line.separator"));
            writer.flush();
        } catch (IOException e) {
            logger.info("IOException encountered. Stacktrace: {}", e.toString());
        }
    }
}
