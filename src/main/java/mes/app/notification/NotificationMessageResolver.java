package mes.app.notification;

import mes.domain.entity.Notification;
import org.springframework.stereotype.Component;

@Component
public class NotificationMessageResolver {

    public Notification resolve(BizEvent event) {

        Notification noti = new Notification();
        // ✅ 공통 필수값 (여기서 반드시 세팅)
        noti.setDomain(event.getDomain());
        noti.setAction(event.getAction());
        if (event.getTargetId() != null) {
            noti.setTargetId(event.getTargetId().toString());
        }
        noti.setSenderUserId(event.getUsername());
        noti.setSpjangcd(event.getSpjangcd()); // 🔥 이거 없으면 무조건 터짐
        noti.setReadYn("N");
        noti.setSpjangcd(event.getSpjangcd());

        // 👇 여기서만 분기
        switch (event.getDomain()) {
            case "wm_suju_list_a" -> resolveSuju(event, noti);
            case "WORK" -> resolveWork(event, noti);
            case "SHIP" -> resolveShip(event, noti);
            default -> {
                noti.setTitle("알림");
                noti.setMessage("새로운 이벤트가 발생했습니다.");
            }
        }

        return noti;
    }

    private void resolveSuju(BizEvent event, Notification noti) {
        switch (event.getAction()) {
            case "SAVE" -> {
                noti.setTitle("수주 등록");
                noti.setMessage("수주가 등록되었습니다.");
            }
            case "MODIFY" -> {
                noti.setTitle("수주 수정");
                noti.setMessage("수주가 수정되었습니다.");
            }
            case "CONFIRM" -> {
                noti.setTitle("수주 확정");
                noti.setMessage("수주가 확정되었습니다.");
            }
            default -> {
                noti.setTitle("수주 알림");
                noti.setMessage("수주 관련 이벤트가 발생했습니다.");
            }
        }
    }

    private void resolveWork(BizEvent event, Notification noti) {
        noti.setTitle("작업 알림");
        noti.setMessage("작업 상태가 변경되었습니다.");
    }

    private void resolveShip(BizEvent event, Notification noti) {
        noti.setTitle("출하 알림");
        noti.setMessage("출하 처리가 완료되었습니다.");
    }
}
