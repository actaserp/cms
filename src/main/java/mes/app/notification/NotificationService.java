package mes.app.notification;


import io.micrometer.core.instrument.util.StringUtils;
import lombok.RequiredArgsConstructor;
import mes.domain.entity.Notification;
import mes.domain.entity.User;
import mes.domain.repository.NotificationRepository;
import mes.domain.repository.UserRepository;
import mes.domain.services.SqlRunner;
import mes.sse.Service.SseService;
import mes.sse.SseController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class NotificationService {

    @Autowired
    SqlRunner sqlRunner;

    private final NotificationRepository notificationRepository;
    private final SseService sseService;
    private final UserRepository userRepository;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void save(Notification base, String receiverUserId) {

        // 🔥 보낸 사람 = 받은 사람이면 알림 생성 자체를 안 함
        if (base.getSenderUserId().equals(receiverUserId)) {
            return;
        }

        Notification noti = new Notification();
        noti.setDomain(base.getDomain());
        noti.setAction(base.getAction());
        noti.setTargetId(base.getTargetId());
        noti.setTitle(base.getTitle());
        noti.setMessage(base.getMessage());
        noti.setSenderUserId(base.getSenderUserId());
        noti.setReceiverUserId(receiverUserId);
        noti.setSpjangcd(base.getSpjangcd());
        noti.setReadYn("N");

        // 1️⃣ DB 저장
        Notification saved = notificationRepository.save(noti);
        notificationRepository.flush();

        // 2️⃣ SSE 전송
        sseService.sendNotification(saved);
    }


    @Transactional(readOnly = true)
    public List<Notification> getUnread(String userId, String spjangcd) {

        List<Notification> list =
                notificationRepository
                        .findByReceiverUserIdAndReadYnAndSpjangcdOrderByCreatedAtDesc(
                                userId, "N", spjangcd
                        );

        for (Notification n : list) {
            userRepository.findByUsername(n.getSenderUserId())
                    .ifPresent(u -> n.setSenderUserName(u.getFirst_name()));
        }

        return list;
    }

    // 사용자 리스트 조회
    public List<Map<String, Object>> getUserList(Integer group, String keyword, String spjangcd){

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("group", group);
        dicParam.addValue("keyword", keyword);
        dicParam.addValue("spjangcd", spjangcd);

        String sql = """
			select au.id
			  , au.first_name
              , up."Name"
              , au.username as login_id
              , up."UserGroup_id"
              , au.email
              , au.tel
              , au.personid
              , ug."Name" as group_name
              , up."Factory_id"
              , f."Name" as factory_name
              , d."Name" as dept_name
              , up."Depart_id"
              , up.lang_code
              , au.is_active
              , to_char(au.date_joined ,'yyyy-mm-dd hh24:mi') as date_joined
              , au.spjangcd as spjangcd
            from auth_user au
            left join user_profile up on up."User_id" = au.id and up.spjangcd = au.spjangcd
            left join user_group ug on ug.id = up."UserGroup_id" and ug.spjangcd = up.spjangcd
            left join factory f on f.id = up."Factory_id" and f.spjangcd = up.spjangcd
            left join depart d on d.id = up."Depart_id" and d.spjangcd = up.spjangcd
            where au.spjangcd = :spjangcd
		    """;

        if (group!=null){
            sql+= " and ug.\"id\" = :group ";
        }

        if (StringUtils.isEmpty(keyword)==false) {
            sql += " and up.\"Name\" like concat('%%', :keyword, '%%') ";
        }

        sql += "order by ug.\"Name\", up.\"Name\"";

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        return items;
    }

    public List<Map<String, Object>> getHistoryList(String startDate, String endDate, String userid, String spjangcd){

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("startDate", startDate);
        dicParam.addValue("endDate", endDate);
        dicParam.addValue("userid", userid);
        dicParam.addValue("spjangcd", spjangcd);

        String sql = """
        SELECT
              n.noti_id,
              n.domain,
              n.action,
              n.title,
              n.message,
          
              n.sender_user_id,
              su.first_name AS sender_user_name,     -- 🔥 보낸 사람 이름
          
              n.receiver_user_id,
              ru.first_name AS receiver_user_name,   -- 🔥 받은 사람 이름
          
              n.parent_noti_id,
              n.read_yn,
          
              TO_CHAR(
                  n.created_at AT TIME ZONE 'Asia/Seoul',
                  'YYYY-MM-DD HH24:MI:SS'
              ) AS created_at,
          
              n.spjangcd
          FROM notification n
          
          -- 🔹 보낸 사람
          LEFT JOIN auth_user su
                 ON su.username = n.sender_user_id
                AND su.spjangcd = n.spjangcd
          
          -- 🔹 받은 사람
          LEFT JOIN auth_user ru
                 ON ru.username = n.receiver_user_id
                AND ru.spjangcd = n.spjangcd
          
          WHERE n.spjangcd = :spjangcd
            AND n.created_at >= TO_TIMESTAMP(:startDate || ' 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
            AND n.created_at <= TO_TIMESTAMP(:endDate   || ' 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
            AND (
                  n.sender_user_id = :userid
               OR n.receiver_user_id = :userid
            )
          ORDER BY n.created_at DESC;
		    """;


        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        return items;
    }
}
