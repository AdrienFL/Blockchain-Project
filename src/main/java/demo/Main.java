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

    public static void main(String[] args) throws InterruptedException, IOException {
        File file = new File("time.csv");
        if (!file.exists()) {
            file.createNewFile();
        }

        int sizes[] = new int[] { 3, 10, 100 };
        int faulty[] = new int[] { 1, 4, 49 };
        long tl[] = new long[] { 500, 1000, 1500, 2000 };
        double alpha[] = new double[] { 0, 0.1, 1. };

        for (int i = 0; i < sizes.length; i++) {
            int N = sizes[i];
            int f = faulty[i];
            for (int j = 0; j < tl.length; j++) {
                for (int k = 0; k < alpha.length; k++) {
                    ArrayList<ActorRef> references = initSystem(N, f, alpha[k]);
                    latch = new CountDownLatch(N); // Initialize latch for N actors
                    cons(references, N, tl[j], alpha[k]);
                    latch.await(); // Wait until all actors report their decision
                    System.out.println("Consensus reached for N=" + N + ", tl=" + tl[j] + ", alpha=" + alpha[k]);
                    clearSystem(references);
                }
            }
        }
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

    private static ArrayList<ActorRef> initSystem(int N, int f, double alpha) {
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

        Collections.shuffle(references);

        for (int i = 0; i < rd.nextInt(f); i++) {
            // send faulty message
        }

        return references;
    }

    public static void cons(ArrayList<ActorRef> references, int N, long tl, double alpha) throws IOException {
        long times[] = new long[10];
        for (int i = 0; i < times.length; i++) {
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
