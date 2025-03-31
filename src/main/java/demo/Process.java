package demo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class Process extends UntypedAbstractActor {
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);// Logger attached to actor
    private final int N;//number of processes
    private final int id;//id of current process
    private Members processes;//other processes' references
    private Integer proposal;
    private int ballot;
    private int readballot;
    private int imposeballot;
    private int estimate;
    private HashMap<ActorRef, Pair> states = new HashMap<>();
    private HashMap<Integer, Integer> ackMajorityMap = new HashMap<>();
    private boolean silentMode = false;
    private boolean faultprone = false;
    private double alpha = 0;
    private boolean proposemode = true;
    private int randomValue;
    private ArrayList<Integer> abortList = new ArrayList<Integer>();

    public Process(int ID, int nb) {
        N = nb;
        id = ID;
        readballot = 0;
        imposeballot = id - N;
        ballot = id-N;
    }

    public String toString() {
        return "Process{" + "id=" + id ;
    }

    /**
     * Static function creating actor
     */
    public static Props createActor(int ID, int nb) {
        return Props.create(Process.class, () -> {
            return new Process(ID, nb);
        });
    }

    private void mayCrash(){
        if (this.faultprone && !this.silentMode){
            Random r = new Random();
            if(r.nextDouble() < alpha){
                log.info("p" + self().path().name() + " crashed");
                this.silentMode = true;
            }
        }
    }

    private void abortReceived(int newBallot, ActorRef pj){
        this.mayCrash();
        if(!this.silentMode){
            //log.info("abort received from " + pj.path().name() + " with ballot " + newBallot);
            this.abortList.add(newBallot);
            this.proposemode = false;
            return;
        }
    }

    private void readReceived(int newBallot, ActorRef pj) {
        this.mayCrash();
        if (!this.silentMode){
            //log.info("read received from " + pj.path().name() + " with ballot " + newBallot);
            if (this.readballot > newBallot || this.imposeballot > newBallot) {
                this.mayCrash();
                pj.tell(new AbortMsg(newBallot), self());
            } else {
                this.readballot = newBallot;
                this.mayCrash();
                pj.tell(new GatherMsg(newBallot, this.imposeballot, this.estimate), self());
            }
        }
    }

    private void gatherReceived(int newBallot, int estballot, int est, ActorRef pj){
        this.mayCrash();
        if (!this.silentMode){
            //log.info("gather received from " + getSender().path().name() + " with ballot " + newBallot);
            if(!this.abortList.contains(newBallot)){
                this.states.put(pj, new Pair(est, estballot));
                if (this.states.size()> N/2){
                    getContext().system().scheduler().scheduleOnce(Duration.ofMillis(0), getSelf(), "majority", getContext().system().dispatcher(), ActorRef.noSender());
                }
            }

        }

    }

    private void ackReceived(int newBallot, ActorRef pj){
        this.mayCrash();
        if(!this.silentMode){
            //log.info("ack received from " + pj.path().name() + " with ballot " + newBallot);
            if (!this.abortList.contains(newBallot)){
                if (this.ackMajorityMap.containsKey(newBallot)){
                    this.ackMajorityMap.put(newBallot, this.ackMajorityMap.get(newBallot)+1);
                } else {
                    this.ackMajorityMap.put(newBallot, 1);
                }
                if (this.ackMajorityMap.get(newBallot) > N/2){
                    getContext().system().scheduler().scheduleOnce(Duration.ofMillis(0), getSelf(), "ackmajority", getContext().system().dispatcher(), ActorRef.noSender());
                }

            }

        }
    }

    private void majorityReceived(){
        if(!silentMode){
            //log.info("majority received with ballot " + this.ballot);
            int maxEstBallot = 0;
            ActorRef maxStateProcess = self();
            for (ActorRef p : this.states.keySet()){
                Pair pair = this.states.get(p);
                if (pair.estballot > maxEstBallot){
                    maxStateProcess = p;
                    maxEstBallot = pair.estballot;
                }
            }
            //log.info("max ballot " + maxEstBallot + " from process " + maxStateProcess.path().name());
            if(maxEstBallot>0){
                this.proposal = this.states.get(maxStateProcess).est;
            }
            this.states.clear();
            for (ActorRef m : this.processes.references){
                this.mayCrash();
                m.tell(new ImposeMsg(this.ballot, this.proposal), self());
            }
        }

    }

    private void propose(int v){
        if(!this.silentMode){
            this.proposal = v;
            this.ballot += N;
            this.states.clear();
            for (ActorRef m : this.processes.references){
                this.mayCrash();
                m.tell(new ReadMsg(this.ballot), self());
            }
            getContext().system().scheduler().scheduleOnce(Duration.ofMillis(new Random().nextInt(10)), getSelf(), "propose", getContext().system().dispatcher(), ActorRef.noSender());
        }
    }


    public void onReceive(Object message) throws Throwable {
        if (message instanceof Members) { //save the system's info
            Members m = (Members) message;
            this.processes = m;
            //log.info("p" + self().path().name() + " received processes info");
        }

        else if (message instanceof ReadMsg) {
            ReadMsg m = (ReadMsg) message;
            this.readReceived(m.ballot, getSender());
        }

        else if(message instanceof GatherMsg){
            GatherMsg m = (GatherMsg) message;
            this.gatherReceived(m.ballot, m.imposeballot, m.estimate, getSender());
        }

        else if(message instanceof AbortMsg){
            AbortMsg m = (AbortMsg) message;
            this.abortReceived(m.ballot, getSelf());
        }

        else if(message instanceof AckMsg){
            AckMsg m = (AckMsg) message;
            this.ackReceived(m.ballot, getSelf());
        }

        else if(message instanceof ImposeMsg){
            this.mayCrash();
            if(!this.silentMode){
                ImposeMsg m = (ImposeMsg) message;
                //log.info("impose received " + self().path().name() + " from " + getSender().path().name() + " with ballot " + m.ballot + " and proposal " + m.proposal);
                if (this.imposeballot>m.ballot || this.readballot>m.ballot){
                    this.mayCrash();
                    getSender().tell(new AbortMsg(m.ballot), self());

                } else {
                    this.imposeballot = m.ballot;
                    this.estimate = m.proposal;
                    for (ActorRef p : this.processes.references){
                        this.mayCrash();
                        p.tell(new AckMsg(this.imposeballot), self());
                    }
                }
            }
        }

        else if(message instanceof DecideMsg){
            DecideMsg m = (DecideMsg) message;
            this.decideReceived(m.proposal);
        }

        else if(message instanceof CrashMsg){
            CrashMsg m = (CrashMsg) message;
            this.alpha = m.alpha;
            //log.info("p" + self().path().name() + " entered fault-prone mode");
            this.faultprone = true;
        }

        else if (message instanceof String){
            String m = (String) message;



            if(m.equals("launch")) {
                randomValue = new Random().nextInt(2);
                log.info("p" + self().path().name() + " proposes : " + randomValue);
                propose(randomValue);
            }

            if(m.equals("majority")){
                this.majorityReceived();
            }

            if (m.equals("ackmajority")){
                this.ackMajorityReceived();
            }

            if(m.equals("hold")){
                this.proposemode = false;
            }

            if(m.equals("propose")){
                if(this.proposemode){
                    //log.info("p" + self().path().name() + " proposes : " + this.randomValue);
                    propose(this.randomValue);

                }
            }

            if(m.equals("reset")){

                log.info("p" + self().path().name() + " reset");
                this.abortList.clear();
                this.states.clear();
                this.ackMajorityMap.clear();
                this.silentMode = false;
                this.faultprone = false;
                this.alpha = 0;
                this.proposemode = true;
                this.readballot = 0;
                this.imposeballot = id - N;
                this.ballot = id - N;
            }
        }


    }

    private void decideReceived(int newProposal) {
        this.mayCrash();
        if(!silentMode){
            log.info("decide received with proposal "+ newProposal + " from " + getSender().path().name());

            for (ActorRef m : this.processes.references){
                if(m != self()){
                    this.mayCrash();
                    m.tell(new DecideMsg(newProposal), self());
                }
            }
            silentMode = true;
            this.proposemode = false;
            log.info("\n-------------------------------------\n" + //
                                "p" + self().path().name() + " DECIDED " + newProposal + "\n" + "--------------------------------");

            Main.notifyDecision();

        }


    }

    private void ackMajorityReceived() {
    if(!this.silentMode){
        log.info("ackmajority received with ballot " + this.ballot);


        for (ActorRef m : this.processes.references){
            this.mayCrash();
            m.tell(new DecideMsg(this.proposal), self());
        }
    }
    Main.notifyDecision();
    }
}
