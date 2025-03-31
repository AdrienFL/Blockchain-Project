package demo;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class Main {

    public static FileWriter w;
    static ActorSystem system = null;
    private static final Random rd = new Random();
    private static CountDownLatch latch;

    private static int  N = 100; // Number of processes
    private static int f = 49; // Number of faulty processes

    public static void main(String[] args) throws InterruptedException, IOException {
        File file = new File("time.csv");
        if (!file.exists()) {
            file.createNewFile();
        }
        w = new FileWriter(file);
        w.append("N,tl,alpha,mean\n");
        w.flush();

        int sizes[] = new int[] { 3, 10, 100 };
        int faulty[] = new int[] { 1, 4, 49 };
        long tl[] = new long[] { 500, 1000, 1500, 2000 };
        double alpha[] = new double[] { 0, 0.1, 1. };


        ArrayList<ActorRef> references = initSystem(N);
        for (int j = 0; j < tl.length; j++) {
            for (int k = 0; k < alpha.length; k++) {
                    cons(references,f, N,tl[j], alpha[k]);
            }
        }

        clearSystem(references);
        w.close();
        System.out.println("done");


    }

    public static void notifyDecision() {
        latch.countDown(); // Decrement the latch count when an actor decides
    }

    private static void clearSystem(ArrayList<ActorRef> references) {
        for (ActorRef actor : references) {
            system.stop(actor);
        }
        system.terminate();
        system = null;
        references.clear();
    }

    private static ArrayList<ActorRef> initSystem(int N) {
        // Instantiate an actor system
        system = ActorSystem.create("System" + N);
        system.log().info("System started with N=" + N);
        ArrayList<ActorRef> references = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            // Instantiate processes
            final ActorRef a = system.actorOf(Process.createActor(i + 1, N), "" + i);
            references.add(a);
        }

        // give each process a view of all the other processes
        Members m = new Members(references);
        for (ActorRef actor : references) {
            actor.tell(m, ActorRef.noSender());
        }

        return references;
    }

    public static void cons(ArrayList<ActorRef> references, int f, int N, long tl, double alpha) throws IOException {
        long times[] = new long[10];
        // Create a latch to wait for all actors to reach consensus
        latch = new CountDownLatch(N - f);
        for (int i = 0; i < times.length; i++) {
            system.log().info("\n-------------------------------------------------Starting consensus for N=" + N + ", tl=" + tl + ", alpha=" + alpha+"-------------------------------------------------\n");
            Collections.shuffle(references);
            // crash some processes
            for (int j = 0; j < rd.nextInt(f); j++) {
                references.get(i).tell(new CrashMsg(alpha), ActorRef.noSender());
            }

            long start = System.currentTimeMillis();

            for (ActorRef actor : references) {
                actor.tell("launch", ActorRef.noSender());
            }

            ActorRef leader = references.get(rd.nextInt(N));
            for (ActorRef actor : references) {
                if (actor != leader) {
                    system.scheduler().scheduleOnce(Duration.ofMillis(tl), actor, "foo", system.dispatcher(), null);
                }
            }

            // Wait for consensus to complete
            try {
                latch.await();
            } catch (InterruptedException e) {
                system.log().error("Error waiting for consensus: " + e.getMessage());
            }

            times[i] = System.currentTimeMillis() - start;

            system.log().info("\n-------------------------------------------------------------Consensus reached for N=" + N + ", tl=" + tl + ", alpha=" + alpha + "-------------------------------------------------------------\n");
            for (ActorRef actor : references) {
                actor.tell("reset", ActorRef.noSender());
            }

        }

        // write statistics to file

        double mean = 0;
        for (int i = 0; i < times.length; i++) {
            mean += times[i];
        }
        mean = mean / times.length;
        w.append(N + "," + tl + "," + alpha + "," + mean + "\n");
        w.flush();
    }

}
