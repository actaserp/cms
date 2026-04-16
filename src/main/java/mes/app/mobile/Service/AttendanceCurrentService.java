package mes.app.mobile.Service;

import mes.domain.entity.approval.TB_E080_PK;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class AttendanceCurrentService {
    @Autowired
    SqlRunner sqlRunner;

    // 사용자 연차정보 조회
    public Map<String, Object> getAnnInfo(int personId) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("personid", personId);

        String sql = """
                SELECT t.ewolnum,
                    t.holinum,
                    t.daynum,
                    t.restnum,
                    p.rtdate
                FROM tb_pb209 t
                LEFT JOIN person p ON p.id = t.personid
                WHERE personid = :personid
        		""";

        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

        return item;
    }
    // 사용자 휴가정보 조회
    public List<Map<String, Object>> getVacInfo(String workcd, String searchYear, int personId) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("personid", personId);
        dicParam.addValue("workcd", workcd);
        dicParam.addValue("searchYear", searchYear);

        String sql = """
                SELECT t.reqdate,
                    t.id,
                    t.workcd,
                    i.worknm,
                    t.yearflag,
                    t.frdate,
                    t.sttime,
                    t.edtime,
                    t.todate,
                    t.daynum,
                    t.remark,
                    t.appgubun,
                    t.fixflag
                FROM tb_pb204 t
                LEFT JOIN tb_pb210 i ON t.workcd = i.workcd 
                WHERE personid = :personid
        		""";
        if(workcd != null && !workcd.isEmpty()){
            dicParam.addValue("workcd", workcd);
            sql += " AND t.workcd = :workcd";
        }
        if (searchYear != null && !searchYear.isEmpty()) {
            dicParam.addValue("searchYear", searchYear);
            sql += " AND LEFT(t.reqdate, 4) = :searchYear";
        }
        sql += " ORDER BY t.reqdate DESC";

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        return items;
    }
    // 휴가결재데이터 조회(appnum)
    public Map<String, Object> getAppInfo(String appnum) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("appnum", appnum);

        String sql = """
                SELECT *
                FROM tb_e080
                WHERE appnum = :appnum
        		""";

        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

        return item;
    }
}
