public class Node<T> {
    public T data;
    public Node<T> prev;
    public Node<T> next;

    public Node(T data) {
        this.data = data;
        this.prev = null;
        this.next = null;
    }
}
