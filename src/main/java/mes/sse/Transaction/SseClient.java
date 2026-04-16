package mes.sse.Transaction;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;

@Slf4j
public class SseClient {

    private final String userId;
    private final SseEmitter emitter;

    public SseClient(String userId, SseEmitter emitter) {
        this.userId = userId;
        this.emitter = emitter;
    }

    public String getUserId() {
        return userId;
    }

    public void send(String eventName, Object data) {
        try {
            emitter.send(SseEmitter.event()
                    .name(eventName)
                    .data(data));
        } catch (IOException e) {
            emitter.completeWithError(e);
        }
    }

    public void complete() {
        emitter.complete();
    }
}
