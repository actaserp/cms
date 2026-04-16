package mes.app.mobile.Service;

import mes.app.common.TenantContext;
import mes.domain.entity.commute.TB_PB201;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class MobileMainService {
    @Autowired
    SqlRunner sqlRunner;

    // 사용자 정보 조회
    public Map<String, Object> getUserInfo(String username) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("username", username);

        String sql = """
                SELECT
                          a.first_name,
                          t.starttime,
                          d."Name",
                          s."Value" as jik_id
                      FROM auth_user a
                      LEFT JOIN tb_pb201 t ON t.personid = a.personid
                      left join person p on p.id = a.personid
                      LEFT JOIN depart d ON p."Depart_id" = d.id
                      left join (
                            SELECT "Code", "Value"
                            FROM sys_code
                            WHERE "CodeType" = 'jik_type'
                    ) s on s."Code" = p.jik_id
                      WHERE a.username = :username
                      ORDER BY t.starttime DESC
                      LIMIT 1
        		""";


        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

        return item;
    }
    // 지각여부 확인위해 근태설정 근무시간 확인 (근무구분에 따라서 starttime 개별적용)
    public Map<String, Object> getWorkTime(String workType) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("workType", workType);

        String sql = """
                SELECT sttime,
                endtime,
                ovsttime,
                ovedtime,
                ngsttime,
                ngedtime
                FROM tb_pbcont
                WHERE flag = :workType
        		""";


        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

        return item;
    }
    // 사용자 출근시간 조회
    public Map<String, Object> getInOfficeTime(String username) {

        String tenantId = TenantContext.get();

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("username", username);
        dicParam.addValue("spjangcd", tenantId);

        String sql = """
                SELECT starttime,
                workcd
                FROM auth_user a
                LEFT JOIN tb_pb201 t
                ON t.personid = a.personid
                WHERE a.username = :username
                and t.spjangcd = :spjangcd
                AND LPAD(EXTRACT(DAY FROM CURRENT_DATE)::TEXT, 2, '0') = workday
                AND TO_CHAR(CURRENT_DATE, 'YYYYMM') = workym;
        		""";


        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

        return item;
    }
    // 직원코드 조회
    public Map<String, Object> getPersonId(String username) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("username", username);

        String sql = """
                SELECT
                    a.personid,
                    p."PersonGroup_id"
                FROM auth_user a
                LEFT JOIN person p
                ON p.id = a.personid
                WHERE a.username = :username
        		""";


        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

        return item;
    }
}
