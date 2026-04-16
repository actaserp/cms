package mes.sse.Transaction;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class SseSubject {

    // spjangcd → 여러 client
    private final Map<String, List<SseClient>> clients = new ConcurrentHashMap<>();

    public void addObserver(String spjangcd, SseClient client) {
        clients
                .computeIfAbsent(spjangcd, k -> new CopyOnWriteArrayList<>())
                .add(client);
    }

    public void removeObserver(String spjangcd, SseClient client) {
        List<SseClient> list = clients.get(spjangcd);
        if (list != null) {
            list.remove(client);
            if (list.isEmpty()) {
                clients.remove(spjangcd);
            }
        }
    }

    /** 🔔 SYSTEM : spjangcd 전체 */
    public void notifySystem(String spjangcd, String message) {
        List<SseClient> list = clients.get(spjangcd);
        if (list == null) return;

        for (SseClient client : list) {
            client.send("SYSTEM", message);
        }
    }

    /** 🔔 개인 알림 */
    public void notifyUser(String spjangcd, String userId,
                           String eventName, Object data) {

        List<SseClient> list = clients.get(spjangcd);
        if (list == null) return;

        for (SseClient client : list) {
            if (!client.getUserId().equals(userId)) continue;
            client.send(eventName, data);
        }
    }
}
