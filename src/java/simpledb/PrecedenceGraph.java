package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class PrecedenceGraph {

    private Map<TransactionId, Set<TransactionId>> forward;  // head to tail edges
    private Map<TransactionId, Set<TransactionId>> backward;  // tail to head edges

    public PrecedenceGraph() {
        this.forward = new HashMap<>();
        this.backward = new HashMap<>();
    }

    public void addTransaction(TransactionId tid) {
        forward.put(tid, new HashSet<>());
        backward.put(tid, new HashSet<>());
    }

    public void removeTransaction(TransactionId tid) {
        for (TransactionId t: forward.get(tid)) {
            backward.get(t).remove(tid);
        }
        for (TransactionId t: backward.get(tid)) {
            forward.get(t).remove(tid);
        }
        forward.remove(tid);
        backward.remove(tid);
    }

    public synchronized void addDependency(TransactionId from, TransactionId to) throws DeadlockException {
        System.out.println("Adding edge from " + from + " to " + to);
        if (hasPath(to, from)) {
            throw new DeadlockException();
        }
        forward.get(from).add(to);
        backward.get(to).add(from);
    }

    public synchronized boolean hasPath(TransactionId from, TransactionId to) {
        // BFS algorithm with dynamic programming
        Queue<TransactionId> queue = new LinkedList<>();
        Set<TransactionId> visited = new HashSet<>();
        queue.add(from);
        while (!queue.isEmpty()) {
            TransactionId tid = queue.remove();
            if (tid.equals(to)) {
                return true;
            }
            if (visited.contains(tid)) {
                continue;
            }
            visited.add(tid);
            queue.addAll(forward.get(tid));
        }
        return false;
    }

}
