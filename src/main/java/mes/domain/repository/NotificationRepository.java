package mes.domain.repository;

import mes.domain.entity.Notification;
import org.apache.ibatis.annotations.Param;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface NotificationRepository extends JpaRepository<Notification, Integer> {
    List<Notification> findByReceiverUserIdAndReadYnAndSpjangcdOrderByCreatedAtDesc(
            String receiverUserId,
            String readYn,
            String spjangcd
    );

    @Modifying
    @Query("""
        update Notification n
           set n.readYn = 'Y'
         where n.notiId = :notiId
    """)
    void markAsRead(@Param("notiId") Integer notiId);

    @Modifying
    @Query("""
        update Notification n
           set n.readYn = 'Y'
         where n.readYn = 'N'
           and n.spjangcd = :spjangcd
           and n.receiverUserId = :userId
    """)
    int markAllAsRead(@Param("spjangcd") String spjangcd, @Param("userId") String userId);
}