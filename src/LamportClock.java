public class LamportClock {

    private long time = 0;

    // Local event
    public synchronized long tick() {
        time++;
        return time;
    }

    // Send event
    public synchronized long sendEvent() {
        time++;
        return time;
    }

    // Receive event
    public synchronized long receiveEvent(long receivedTime) {
        time = Math.max(time, receivedTime) + 1;
        return time;
    }

    public synchronized long getTime() {
        return time;
    }
}