package core;

import com.sun.org.glassfish.external.statistics.Stats;
import core.concurrent.SEPExecutor;
import core.concurrent.SharedExecutorPool;
import core.generators.FileGenerator;
import core.utils.Hashing;
import core.utils.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LatBenchMarker {

    static List<SEPExecutor> servers = new ArrayList<SEPExecutor>();
    static int serverCount=3;
    static String queueType = "MultiQueueDRR2";
    static String strategy = "hybrid";
    static int initialRange = 50;
    static int rangeFactor = 5;
    static int numOps = 10000;
    static int numProds = 1;
    static int numCons = 1;
    static FileGenerator traceFile= new FileGenerator("/Users/reda/git/cicero/trace-processing/second_simulatorTrace");
    static float util = 0.75f; //system utilization (overrides interarrival time)
    static int interarrival = 10; //work interarrival time in ms
    static int serviceTime = 10; //service time in ms
    static List<Integer> weights = Arrays.asList(2000, 4, 3, 2, 1);

    static ConcurrentHashMap<String, FileWriter> filePointers = new ConcurrentHashMap<String, FileWriter>();
    private static final Logger logger = LoggerFactory.getLogger(LatBenchMarker.class);

    private static void initialize()
    {
        //calculate interarrival time
        float serviceRate = 1/(float)interarrival;

        //FIXME: calculate the average request size (for now, it's hardcoded)
        interarrival = (int)(1/((serviceRate*numCons*serverCount)/numProds*util)*8);

        logger.info("Interarrival time: {}", interarrival);

        //initializing servers
        for(int i=0; i<serverCount; i++)
        {
            Queue q;
            SharedExecutorPool sep = new SharedExecutorPool("Server-"+Integer.toString(i));
            switch(queueType) {
                case "MultiQueueDRR":
                    q = new MultiConcurrentLinkedPriorityQueueDRR(weights);
                    break;
                default:
                    q = new ConcurrentLinkedQueue();
                    break;
            }

            //create a threadpool for each server
            //TODO pin these threadpools to different cores to avoid contention
            servers.add(new SEPExecutor(sep, numCons, Integer.MAX_VALUE, "", "ServerExecutor-"+Integer.toString(i), q));
        }

        //create log directory
        final File logDir = new File("temp");

        if (null != logDir) {
            logDir.mkdirs();
        }
    }

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

    /**
     * Reads and parses next line in the workload trace
     * @throws UnsupportedOperationException
     */
    private static List<String> readTransactionFromFile(FileGenerator filegen) throws UnsupportedOperationException
    {
        String line = filegen.nextString();
        if(line == null)
            return null;
        List<String> task;
        line.replaceAll("\n", "");
        if(line.startsWith("R "))
            task = new ArrayList(Arrays.asList(line.split(" ")));
        else
            throw new UnsupportedOperationException();
        task.remove(0); //remove R
        task.remove(task.size()-1); //remove interarrival modifier (no longer used)
        return task;
    }

    private static double calculateAverage(Queue<Integer> marks) {
        long sum = 0;

        while(marks.peek() != null)
            sum += marks.poll();

        return marks.isEmpty()? 0: 1.0*sum/marks.size();
    }

    private static void produceLogs()
    {
        //close all files
        for(FileWriter fw: filePointers.values())
        {
            try {
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        File folder = new File("temp");
        File[] listOfFiles = folder.listFiles();
        File latencyFile = new File("latencies.csv");
        latencyFile.delete();
        mergeFiles(listOfFiles, latencyFile);
        System.out.println("Detailed latency results have been saved to latency.csv");
    }

    private static void mergeFiles(File[] files, File mergedFile) {

        FileWriter fstream = null;
        BufferedWriter out = null;
        try {
            fstream = new FileWriter(mergedFile);
            out = new BufferedWriter(fstream);
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        for (File f : files) {
            //System.out.println("merging: " + f.getName());
            FileInputStream fis;
            try {
                fis = new FileInputStream(f);
                BufferedReader in = new BufferedReader(new InputStreamReader(fis));

                String aLine;
                while ((aLine = in.readLine()) != null) {
                    out.write(aLine);
                    out.newLine();
                }

                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            //delete temp file
            f.delete();
        }

        try {
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String args[]) throws InterruptedException {

        initialize();

        ExecutorService producers = Executors.newFixedThreadPool(numProds);

        final AtomicInteger taskId = new AtomicInteger(0);
        final AtomicInteger opCount = new AtomicInteger(0);
        for(int i=0; i<numProds; i++) {
            producers.submit(new Runnable() {
                public void run() {
                    //code to run in parallel
                    try {
                        logger.info("Before: {}", System.currentTimeMillis());
                        for (int i = 0; i < numOps; i++) {
                            long startTime = System.currentTimeMillis();
                            int id = taskId.addAndGet(1);
                            int maxRGCount = 0;
                            int maxRG = 0;
                            List<String> task = readTransactionFromFile(traceFile);

                            //System.out.println(task.toString());
                            Map<Integer, Integer> taskCounts = new HashMap<Integer, Integer>();
                            Map<Integer, Integer> taskPriorities = new HashMap<Integer, Integer>();
                            for (int j = 0; j < task.size(); j++) {
                                int keyInt = (int) Hashing.hash(Integer.parseInt(task.get(j)));
                                keyInt = keyInt % 100000;
                                if (keyInt < 0) keyInt += (100000);
                                int serverChoice = keyInt % serverCount;
                                //logger.info("Server choice is: {} {}", serverChoice, keyInt);
                                if (taskCounts.containsKey(serverChoice))
                                    taskCounts.put(serverChoice, taskCounts.get(serverChoice) + 1);
                                else
                                    taskCounts.put(serverChoice, 1);

                                if (taskCounts.get(serverChoice) > maxRGCount) {
                                    maxRGCount = taskCounts.get(serverChoice);
                                    maxRG = serverChoice;
                                }
                            }

                            int range = initialRange;
                            int maxWeight = Collections.max(weights);

                            if (strategy.equals("hybrid") || strategy.equals("SBF")) {
                                for (Integer qw : weights) {
                                    if (maxRGCount <= range) {
                                        maxWeight = qw;
                                        break;
                                    }
                                    range = range * rangeFactor;
                                }
                            }

                            taskPriorities.put(maxRG, maxWeight);

                            for (int rg : taskCounts.keySet()) {
                                int priority = maxWeight;
                                if (queueType.equals("MultiQueueDRR")) {
                                    if (strategy.equals("hybrid") || strategy.equals("SDS")) {
                                        if (maxRG != rg) {
                                            float minError = Float.MAX_VALUE; //set min error to arbitrary high value
                                            for (Integer qw : weights) {
                                                float rgSize = taskCounts.get(rg);

                                                //Search for weights that minimize distance
                                                float error = Math.abs(rgSize / maxRGCount - qw / maxWeight);
                                                if (error < minError && qw <= maxWeight) {
                                                    minError = error;
                                                    priority = qw;
                                                }
                                            }
                                        }
                                    }
                                }
                                taskPriorities.put(rg, priority);
                            }

                            //System.out.println(taskCounts.toString());
                            // Trigger requests
                            for (int rg : taskCounts.keySet()) {
                                for (int l=0; l<taskCounts.get(rg); l++) {
                                    //logger.info("RG: {}. OpCount: {}", rg, opCount.addAndGet(1));
                                    //logger.info("Executing task on server: {} {}", rg, taskPriorities.keySet());
                                    opCount.addAndGet(1);
                                    servers.get(rg).maybeExecuteImmediately(new Operation(id, rg, taskPriorities.get(rg), serviceTime, filePointers));
                                    //servers.get(rg).submit(new Operation(id, rg, taskPriorities.get(rg), serviceTime));
                                }
                            }

                            //FIXME: Sleep durations aren't accurate for smaller timescales; consider busy-waiting instead
                            Thread.sleep(Math.max(interarrival - (System.currentTimeMillis() - startTime), 0));
                        }
                    } catch (Exception x) {
                        System.out.println("Caught it" + extrapolateStackTrace(x));
                    }
                }
            });
        }

        // once you've submitted your last job to the service it should be shut down
        producers.shutdown();

        // wait for the threads to finish if necessary
        producers.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

        logger.info("After: {}", System.currentTimeMillis());

        logger.info("Producers have stopped");

        boolean workComplete = false;
        while(!workComplete) {
            for (SEPExecutor s : servers) {
                if (s.getPendingTasks() == 0 && s.getActiveCount() == 0)
                    workComplete = true;
                else
                    workComplete = false;
            }
        }

        for (SEPExecutor s : servers) {
            s.shutdown();
            s.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }

        logger.info("Consumers have stopped");

        logger.info("Total # of ops: {}", opCount.get());

        produceLogs();
    }
}
