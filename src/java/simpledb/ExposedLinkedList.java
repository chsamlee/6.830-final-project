package simpledb;

public class ExposedLinkedList<T> {

    public class Node {
        private T val;
        private Node prev;
        private Node next;

        private Node(T v) {
            val = v;
        }

        public T val() {
            return val;
        }

        public Node prev() {
            return (prev == head ? null : prev);
        }

        public Node next() {
            return (next == tail ? null : next);
        }
    }

    private Node head;
    private Node tail;

    public ExposedLinkedList() {
        head = new Node(null);
        tail = new Node(null);
        link(head, tail);
    }

    public boolean isEmpty() {
        return (head.next == tail);
    }

    public Node head() {
        return (isEmpty() ? null : head.next);
    }

    public Node add(T val) {
        Node node = new Node(val);
        link(tail.prev, node);
        link(node, tail);
        return node;
    }

    public void remove(Node node) {
        link(node.prev, node.next);
    }

    public void pop() {
        if (isEmpty()) {
            throw new IllegalStateException("Linked list is empty");
        }
        remove(head());
    }

    public void pushToEnd(Node node) {
        link(node.prev, node.next);
        link(tail.prev, node);
        link(node, tail);
    }

    private void link(Node before, Node after) {
        before.next = after;
        after.prev = before;
    }

}
