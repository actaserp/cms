package mes.app.mobile.Service;

import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class AttendanceStatisticsService {
    @Autowired
    SqlRunner sqlRunner;

    // 차트 데이터 조회
    public List<Map<String, Object>> getVacInfo(String username, String searchYear) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("username", username);

        dicParam.addValue("searchYear", searchYear);

        String sql = """
                SELECT
                    t.workcd,
                    tn.worknm AS HISNM,
                    SUM(t.daynum) AS HISPOINT
                FROM tb_pb204 t
                LEFT JOIN tb_pb210 tn ON t.workcd = tn.workcd
                WHERE t.appuserid = :username
                  AND LEFT(t.reqdate, 4) = :searchYear
                GROUP BY t.workcd, tn.worknm
        		""";

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        return items;
    }
    // 유저정보 조회
    public Map<String, Object> getUserInfo(String username) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("username", username);

        String sql = """
                SELECT
                    a.username,
                    a.first_name,
                    p."Name",
                    p.jik_id,
                    s."Value" as jiknm,
                    d."Name" as departnm,
                    p."Depart_id"
                FROM auth_user a
                LEFT JOIN person p ON p.id = a.personid
                LEFT JOIN sys_code s ON p.jik_id = s."Code" AND "CodeType" = 'jik_type'
                LEFT JOIN depart d ON p."Depart_id" = d.id
                WHERE a.username = :username
        		""";

        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

        return item;
    }
}
