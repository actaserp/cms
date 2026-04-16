package mes.app.notification;

import lombok.RequiredArgsConstructor;
import mes.domain.entity.Notification;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.List;

@Component
@RequiredArgsConstructor
public class BizEventListener {

    private final NotificationService notificationService;
    private final NotificationMessageResolver resolver;
    private final NotificationTargetService targetService;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handle(BizEvent event) {

        // 1️⃣ 알림 템플릿 생성
        Notification base = resolver.resolve(event);

        // 2️⃣ 수신자 목록 조회 (A 권한)
        List<String> receivers =
                targetService.findReceivers(event.getDomain(), event.getSpjangcd());

        // 3️⃣ 수신자별 알림 생성 & 저장
        for (String receiverUserId : receivers) {

            // 본인은 제외하고 싶으면 여기서 컷
            if (receiverUserId.equals(event.getUsername())) {
                continue;
            }

            notificationService.save(base, receiverUserId);
        }

    }
}
