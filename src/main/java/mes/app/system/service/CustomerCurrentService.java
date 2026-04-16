package mes.app.system.service;

import lombok.extern.slf4j.Slf4j;
import mes.app.common.TenantContext;
import mes.domain.entity.Tb_xa012;
import mes.domain.entity.User;
import mes.domain.entity.UserGroup;
import mes.domain.model.AjaxResult;
import mes.domain.repository.Tb_xa012Repository;
import mes.domain.repository.UserGroupRepository;
import mes.domain.repository.UserRepository;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class CustomerCurrentService {

    @Autowired
    SqlRunner sqlRunner;

    @Autowired
    Tb_xa012Repository xa012Repository;

    @Autowired
    UserRepository userRepository;

    @Autowired
    UserGroupRepository userGroupRepository;

    //고객사현황 그리드
    public List<Map<String, Object>> getCustomerList(String srchStartDt, String srchEndDt, String keyword) {
        MapSqlParameterSource param = new MapSqlParameterSource();


        // YYYY-MM-DD → YYYYMMDD
        String st = srchStartDt.replace("-", "");
        String en = srchEndDt.replace("-", "");

        param.addValue("st", st);
        param.addValue("en", en);
        param.addValue("keyword" , "%" + keyword + "%");


        String sql = """
                    select row_number() over (order by x.expirationdate desc) as seq,
                                                            *
                                                            from tb_xa012 x
                    join bill_plans b on b.id = x.bill_plans_id
                    where x."subscriptiondate" between :st and :en
                """;

        if(!keyword.isEmpty())

        {
            sql += """
                and x.spjangnm like :keyword
                """;
        }

        sql += """
                order by x.expirationdate desc
                """;


        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, param);

        return items;
    }

    @Transactional
    public void batchApprove(List<String> spjangcds) {
        List<String> menuCodes = List.of(
                "wm_user_group",
                "wm_amount_used",
                "wm_user",
                "wm_user_group_menu",
                "wm_storyboard_config",
                "wm_prop_master",
                "wm_login_log",
                "wm_menu_log"
        );

        for (String spjangcd : spjangcds) {

            // 1. tb_xa012 state -> 'O', expirationdate -> 다음달
            Tb_xa012 xa012 = xa012Repository.findById(spjangcd)
                    .orElseThrow(() -> new RuntimeException("사업체를 찾을 수 없습니다: " + spjangcd));

            if ("O".equals(xa012.getState())) {
                log.info("이미 승인처리된 사업체입니다: {}", spjangcd);
                continue;
            }

            xa012.setState("O");
            xa012.setExpirationdate(null);
            xa012.setSubscribeunit("month");
            xa012.setBillingdate(25);
            xa012Repository.save(xa012);

            boolean isNew = userGroupRepository.findBySpjangcd(spjangcd).isEmpty();
            if (isNew) {
                // 2. auth_user is_active -> true
                User user = userRepository.findBySpjangcd(spjangcd)
                        .orElseThrow(() -> new RuntimeException("사용자를 찾을 수 없습니다: " + spjangcd));
                user.setActive(true);
                userRepository.save(user);

                // 3. user_group insert
                UserGroup userGroup = new UserGroup();
                userGroup.setCode("Master");
                userGroup.setName("마스터");
                userGroup.setDescription("마스터 그룹");
                userGroup.setGmenu("wm_user_group");
                userGroup.setSpjangcd(spjangcd);
                userGroup.set_audit(user);
                UserGroup savedUserGroup = userGroupRepository.save(userGroup);

                // 4. user_profile insert (jdbc)
                String userProfileSql = """
                INSERT INTO user_profile("_created", "_creater_id", "User_id", "lang_code", "Name", "Factory_id", "Depart_id", "UserGroup_id", "spjangcd")
                VALUES(now(), :user_id, :user_id, :lang_code, :name, null, null, :group_id, :spjangcd)
                """;

                MapSqlParameterSource profileParam = new MapSqlParameterSource();
                profileParam.addValue("user_id", user.getId());
                profileParam.addValue("lang_code", "ko");
                profileParam.addValue("name", user.getUsername());
                profileParam.addValue("group_id", savedUserGroup.getId());
                profileParam.addValue("spjangcd", spjangcd);

                this.sqlRunner.execute(userProfileSql, profileParam);

                // 5. user_group_menu insert (jdbc)
                String sql = """
                INSERT INTO user_group_menu("UserGroup_id", "MenuCode", "AuthCode", _creater_id, _created, spjangcd)
                VALUES(:group_id, :menu_code, :auth_code, :user_id, now(), :spjangcd)
                """;

                for (String menuCode : menuCodes) {
                    MapSqlParameterSource dicParam = new MapSqlParameterSource();
                    dicParam.addValue("spjangcd", spjangcd);
                    dicParam.addValue("auth_code", "RW");
                    dicParam.addValue("menu_code", menuCode);
                    dicParam.addValue("group_id", savedUserGroup.getId());
                    dicParam.addValue("user_id", user.getId());

                    this.sqlRunner.execute(sql, dicParam);
                }
            }
        }
    }

    @Transactional
    public void btnStop(List<String> spjangcds) {

        for (String spjangcd : spjangcds) {

            // 1. tb_xa012 state -> 'O', expirationdate -> 다음달
            Tb_xa012 xa012 = xa012Repository.findById(spjangcd)
                    .orElseThrow(() -> new RuntimeException("사업체를 찾을 수 없습니다: " + spjangcd));


            xa012.setState("중지");
            xa012.setExpirationdate(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")));
            xa012Repository.save(xa012);
        }
    }
}


