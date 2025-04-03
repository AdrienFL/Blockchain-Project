package demo;

import java.util.ArrayList;

import akka.actor.ActorRef;

/**
 * Class containing the processes' references
 */
public class Members {
    public final ArrayList<ActorRef> references;
    public final String data;

    public Members(ArrayList<ActorRef> references) {
        this.references = references;
        String s = "[ ";
        for (ActorRef a : references) {
            s += a.path().name() + " ";
        }
        s += "]";
        data = s;
    }

}
