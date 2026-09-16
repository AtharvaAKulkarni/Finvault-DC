import java.io.Serializable;

public class ClockResponse implements Serializable {
    private final long serverTime;
    private final long lamportTime;
    public ClockResponse(long serverTime, long lamportTime) {
        this.serverTime = serverTime;
        this.lamportTime = lamportTime;
    }
    public long getServerTime() {
        return serverTime;
    }
    public long getLamportTime() {
        return lamportTime;
    }
}