package mes.sse;

import lombok.RequiredArgsConstructor;
import mes.domain.entity.Notification;
import mes.domain.entity.User;
import mes.sse.Transaction.SseClient;
import mes.sse.Transaction.SseSubject;
import org.springframework.http.MediaType;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;

@RequiredArgsConstructor
@RestController
@RequestMapping("/sse")
public class SseController {

    private final SseSubject subject;

    @GetMapping(value = "/subscribe", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter subscribe(@RequestParam String spjangcd,
                                Authentication authentication) {

        User user = (User) authentication.getPrincipal();
        String userId = user.getUsername();

        SseEmitter emitter = new SseEmitter(60 * 60 * 1000L);
        SseClient client = new SseClient(userId, emitter);

        subject.addObserver(spjangcd, client);

        emitter.onCompletion(() -> subject.removeObserver(spjangcd, client));
        emitter.onTimeout(() -> subject.removeObserver(spjangcd, client));
        emitter.onError(e -> subject.removeObserver(spjangcd, client));

        try {
            emitter.send(SseEmitter.event()
                    .name("CONNECTED")
                    .data("connected"));
        } catch (IOException e) {
            emitter.completeWithError(e);
        }

        return emitter;
    }
}

