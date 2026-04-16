package mes.app.report.service;

import mes.domain.repository.JobPlanHeadRepository;
import mes.domain.repository.JobPlanRepository;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class WorkReportService {

    @Autowired
    SqlRunner sqlRunner;

    @Autowired
    JobPlanRepository jobPlanRepository;

    @Autowired
    JobPlanHeadRepository jobPlanHeadRepository;


    // 내역 조회
    public List<Map<String, Object>> getList(String start, String end, String txtName, String spjangcd) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("start", start);
        dicParam.addValue("end", end);
        dicParam.addValue("txtName", txtName);
        dicParam.addValue("spjangcd", spjangcd);

        String sql = """
                SELECT
                    a."rptid" AS id,
                    TO_CHAR(TO_DATE(a.rptdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS rptdate,
                    a."rptweek",
                    a."frdate",
                    a."todate",
                    a."fixperid",
                    a."fixpernm",
                    a."cltnm",

                    a."fixflag",
                    sc1."Value" AS fixflag_nm,

                    a."asmenu",

                    a."actflag",
                    sc2."Value" AS actflag_nm,

                    a."asdv",
                    sc3."Value" AS asdv_nm,

                    a."recyn",
                    sc4."Value" AS recyn_nm,

                    a."rptremark",
                    a."etcremark",
                    a."remark",
                    TO_CHAR(a."inputdate", 'YYYY-MM-DD HH24:MI') AS inputdate

                FROM "tb_as020" a

                LEFT JOIN "sys_code" sc1
                    ON sc1."Code" = a."fixflag"
                   AND sc1."CodeType" = 'fixflag'

                LEFT JOIN "sys_code" sc2
                    ON sc2."Code" = a."actflag"
                   AND sc2."CodeType" = 'actflag'

                LEFT JOIN "sys_code" sc3
                    ON sc3."Code" = a."asdv"
                   AND sc3."CodeType" = 'asdv'

                LEFT JOIN "sys_code" sc4
                    ON sc4."Code" = a."recyn"
                   AND sc4."CodeType" = 'recyn'
                WHERE 1=1
                  AND a.rptdate BETWEEN :start AND :end
            """;

        // txtName 이 있을 때만 조건 추가
        if (txtName != null && !txtName.trim().isEmpty()) {
            sql += " AND fixpernm LIKE CONCAT('%', :txtName, '%') ";
        }

        sql += " ORDER BY fixflag ASC, rptdate DESC ";

        return this.sqlRunner.getRows(sql, dicParam);
    }

    // 수주 상세정보 조회
    public Map<String, Object> getDetail(Integer id) {

        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("id", id);

        String sql = """ 
            SELECT
                a."rptid" AS id,
                TO_CHAR(TO_DATE(a.rptdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS rptdate,
                a."rptweek",
                a."frdate",
                a."todate",
                a."fixperid",
                a."fixpernm",
                a."cltnm",

                a."fixflag",
                sc1."Value" AS fixflag_nm,

                a."asmenu",

                a."actflag",
                sc2."Value" AS actflag_nm,

                a."asdv",
                sc3."Value" AS asdv_nm,

                a."recyn",
                sc4."Value" AS recyn_nm,

                a."rptremark",
                a."etcremark",
                a."remark",
                a."inputdate"

            FROM "tb_as020" a

            LEFT JOIN "sys_code" sc1
                ON sc1."Code" = a."fixflag"
               AND sc1."CodeType" = 'fixflag'

            LEFT JOIN "sys_code" sc2
                ON sc2."Code" = a."actflag"
               AND sc2."CodeType" = 'actflag'

            LEFT JOIN "sys_code" sc3
                ON sc3."Code" = a."asdv"
               AND sc3."CodeType" = 'asdv'

            LEFT JOIN "sys_code" sc4
                ON sc4."Code" = a."recyn"
               AND sc4."CodeType" = 'recyn'
            WHERE a."rptid" = :id
            """;

        Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);

        return item;
    }


}
