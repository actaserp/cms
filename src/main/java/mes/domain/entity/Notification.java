package mes.domain.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "notification")
@Getter
@Setter
public class Notification {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "noti_id")
    private Integer notiId;

    // BizEvent 정보
    private String domain;
    private String action;

    @Column(name = "target_id", nullable = false)
    private String targetId;

    private String title;

    @Column(columnDefinition = "TEXT")
    private String message;

    // 보낸 사람
    @Column(name = "sender_user_id", nullable = false)
    private String senderUserId;

    // 받은 사람
    @Column(name = "receiver_user_id", nullable = false)
    private String receiverUserId;

    // 답장 알림일 경우
    @Column(name = "parent_noti_id")
    private Integer parentNotiId;

    @Column(name = "read_yn", length = 1)
    private String readYn = "N";

    @Column(name = "created_at")
    private LocalDateTime createdAt = LocalDateTime.now();

    private String spjangcd;

    @Transient
    private String senderUserName;   // first_name
}
