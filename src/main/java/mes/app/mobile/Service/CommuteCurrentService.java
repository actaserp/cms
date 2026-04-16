package mes.app.mobile.Service;

import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class CommuteCurrentService {
    @Autowired
    SqlRunner sqlRunner;

    public List<Map<String, Object>> getUserInfo(String username, String workcd, String searchFromDate, String searchToDate) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("username", username);
        dicParam.addValue("workcd", workcd);

        // 날짜 포맷 처리 (yyyy-MM-dd -> yyyyMM, dd)
        String fromYearMonth = searchFromDate.replace("-", "").substring(0, 6); //  yyyymm
        String fromDay = searchFromDate.substring(8, 10); // dd

        String toYearMonth = searchToDate.replace("-", "").substring(0, 6);
        String toDay = searchToDate.substring(8, 10);

        dicParam.addValue("fromYearMonth", fromYearMonth);
        dicParam.addValue("fromDay", fromDay);
        dicParam.addValue("toYearMonth", toYearMonth);
        dicParam.addValue("toDay", toDay);

        String sql = """
                SELECT
                t.workym,
                t.workday,
                t.personid,
                t.worknum,
                t.holiyn,
                t.workyn,
                t.workcd,
                td.worknm,
                t.starttime,
                t.endtime,
                t.worktime,
                t.nomaltime,
                t.overtime,
                t.nighttime,
                t.holitime,
                t.jitime,
                t.jotime,
                t.yuntime,
                t.abtime,
                t.bantime,
                t.adttime01,
                t.adttime02,
                t.adttime03,
                t.adttime04,
                t.adttime05,
                t.adttime06,
                t.adttime07,
                t.remark,
                t.fixflag,
                a.first_name,
                TRIM(BOTH ', ' FROM (
                  CASE WHEN t.jitime = 1 THEN '지각, ' ELSE '' END ||
                  CASE WHEN t.jotime = 1 THEN '조퇴, ' ELSE '' END ||
                  CASE WHEN t.yuntime = 1 THEN '연차, ' ELSE '' END ||
                  CASE WHEN t.abtime = 1 THEN '결근, ' ELSE '' END ||
                  CASE WHEN t.bantime = 1 THEN '반차, ' ELSE '' END
                )) AS status_text
            FROM tb_pb201 t
            LEFT JOIN auth_user a ON a.personid = t.personid
            LEFT JOIN person p ON p.id = a.personid
            LEFT JOIN tb_pb210 td ON t.workcd = td.workcd
            WHERE 1=1
              AND a.username = :username
        		""";

        if(workcd != null && !workcd.isEmpty()){
            sql += " AND t.workcd = :workcd";
        }
        // 날짜 조건 추가
        sql += """
        AND (
            (t.workym > :fromYearMonth)
            OR (
                t.workym = :fromYearMonth AND t.workday >= :fromDay
            )
        )
        AND (
            (t.workym < :toYearMonth)
            OR (
                t.workym = :toYearMonth AND t.workday <= :toDay
            )
        )
    """;

        sql += """
               ORDER BY t.workym DESC, t.workday DESC
               """;



        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        return items;
    }

}
