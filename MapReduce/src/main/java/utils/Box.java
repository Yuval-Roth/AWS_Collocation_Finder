package utils;

public class Box<T> {
    private T value;

    public Box() {}
    public Box(T value) {
        this.value = value;
    }
    public void set(T value) {
        this.value = value;
    }
    public T get() {
        return value;
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
