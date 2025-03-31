package demo;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    
    private void abortReceived(int newBallot, ActorRef pj){
        log.info("abort received " + self().path().name() + " from " + pj.path().name() + " with ballot " + newBallot);
        return;
    }
    
    private void readReceived(int newBallot, ActorRef pj) {
            log.info("read received " + self().path().name() + " from " + pj.path().name() + " with ballot " + newBallot);
            if (readballot > newBallot || imposeballot > newBallot) {
                readballot = newBallot;
                pj.tell(new AbortMsg(newBallot), self());
            } else {
                readballot = newBallot;
                pj.tell(new GatherMsg(newBallot, imposeballot, estimate), self());
            }
    }

    private void gatherReceived(int newBallot, int estballot, int est, ActorRef pj){
        log.info("gather received " + self().path().name() + " from " + getSender().path().name() + " with ballot " + newBallot);
        states.put(pj, new Pair(est, estballot));
        if (states.size()>=N/2){
            getContext().system().scheduler().scheduleOnce(Duration.ofMillis(1000), getSelf(), "majority", getContext().system().dispatcher(), ActorRef.noSender());
        }
    }

    private void ackReceived(int ballot, ActorRef pj){
        log.info("ack received " + self().path().name() + " from " + pj.path().name() + " with ballot " + ballot);
        if (ackMajorityMap.containsKey(ballot)){
            ackMajorityMap.put(ballot, ackMajorityMap.get(ballot)+1);
        } else {
            ackMajorityMap.put(ballot, 1);
        }
        if (ackMajorityMap.get(ballot) >= N/2){
            getContext().system().scheduler().scheduleOnce(Duration.ofMillis(1000), getSelf(), "ackmajority", getContext().system().dispatcher(), ActorRef.noSender());
        }
    }

    private void majorityReceived(){
        log.info("majority received " + self().path().name() + " with ballot " + ballot);
        int maxEstBallot = 0;
        ActorRef maxStateProcess = self();
        for (ActorRef p : states.keySet()){
            Pair pair = states.get(p);
            if (pair.estballot > maxEstBallot){
                maxStateProcess = p;
                maxEstBallot = pair.estballot;
            }
        }
        log.info("max ballot " + maxEstBallot + " from process " + maxStateProcess.path().name());
        if(maxEstBallot>0){
            proposal = states.get(maxStateProcess).est;
            
        }
        states.clear();
        for (ActorRef m : processes.references){
            m.tell(new ImposeMsg(ballot, proposal), self());
        }

    }
    
    private void propose(int v){
        proposal = v;
        ballot += N;
        for (ActorRef m : processes.references){
            m.tell(new ReadMsg(ballot), self());
        }        
    }


    public void onReceive(Object message) throws Throwable {
        if (message instanceof Members) { //save the system's info
            Members m = (Members) message;
            processes = m;
            log.info("p" + self().path().name() + " received processes info");
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
            ImposeMsg m = (ImposeMsg) message;
            log.info("impose received " + self().path().name() + " from " + getSender().path().name() + " with ballot " + m.ballot + " and proposal " + m.proposal);
            if (imposeballot>m.ballot || readballot>m.ballot){
                getSender().tell(new AbortMsg(m.ballot), self());      
                
            } else {
                imposeballot = m.ballot;
                estimate = m.proposal;
                for (ActorRef p : processes.references){
                    p.tell(new AckMsg(imposeballot), self());
                }

            }
        }

        else if(message instanceof DecideMsg){
            DecideMsg m = (DecideMsg) message;
            this.decideReceived(m.proposal);
        }


        else if (message instanceof String){
            String m = (String) message;
            log.info("p" + self().path().name() + " received message: " + m);

            if(m.equals("launch")) {
                int randomValue = new Random().nextInt(1);
                log.info("p" + self().path().name() + " proposes : " + randomValue);
                propose(randomValue);
            }

            if(m.equals("majority")){
                this.majorityReceived();
            }

            if (m.equals("ackmajority")){
                this.ackMajorityReceived();
            }   
          
        }

         
    }

    private void decideReceived(int newProposal) {
        if(!silentMode){
            log.info("decide received " + self().path().name() + " with proposal " + newProposal);
        
            for (ActorRef m : processes.references){
                if(m != self()){
                    m.tell(new DecideMsg(newProposal), self());
                }
            }
            log.info("DECIDED " + newProposal);
        }
        
        silentMode = true;
    }

    private void ackMajorityReceived() {
    if(!silentMode){
        log.info("ackmajority received " + self().path().name() + " with ballot " + ballot);

        
        for (ActorRef m : processes.references){
            m.tell(new DecideMsg(proposal), self());
        }
    }
    }
}
