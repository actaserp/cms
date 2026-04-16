package mes.sse.Transaction;

public interface SseObserver {

    void send(String eventName, Object data);
}
