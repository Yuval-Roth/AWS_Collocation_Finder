package utils;



public class DoublyLinkedList<T> {
    private Node<T> first;
    private Node<T> last;

    public DoublyLinkedList() {
        this.first = null;
        this.last = null;
    }

    // Method to add a new node at the end of the list
    public Node<T> add(T data) {
        Node<T> newNode = new Node<>(data);
        if (first == null) {
            first = newNode;

        } else {
            last.next = newNode;
            newNode.prev = last;
        }
        last = newNode;
        return newNode;
    }

    public Node<T> pop() {
        if (first == null) {
            return null;
        }
        Node<T> node = first;
        first = first.next;
        if (first != null) {
            first.prev = null;
        }
        return node;
    }

    public void remove(Node<T> node) {
        if (node == null) {
            return;
        } else if (node == first) {
            first = node.next;
        } else if (node == last) {
            last = node.prev;
        } else {
            node.prev.next = node.next;
            node.next.prev = node.prev;
        }
    }

    public static class Node<T> {
        public T data;
        public Node<T> prev;
        public Node<T> next;

        public Node(T data) {
            this.data = data;
            this.prev = null;
            this.next = null;
        }
    }
}
