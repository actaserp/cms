package mes.app.notification.comment;

import lombok.RequiredArgsConstructor;
import mes.domain.entity.Notification;
import mes.domain.repository.NotificationRepository;
import mes.sse.Service.SseService;
import org.springframework.stereotype.Service;



@Service
@RequiredArgsConstructor
public class NotificationCommentService {

    private final NotificationRepository repository;
    private final SseService sseservice;

    public void saveAndSend(Notification comment) {

        Notification saved = repository.save(comment);

        // 실시간 전송
        sseservice.sendComment(
                saved
        );
    }
}