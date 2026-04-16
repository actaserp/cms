package mes.app.clock.service;

import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DayMonthlyService {

    @Autowired
    SqlRunner sqlRunner;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public List<Map<String, Object>> getDayList(String work_division, String serchday, String spjangcd,String depart) {
        MapSqlParameterSource paramMap = new MapSqlParameterSource();

        // serchday이 20250514 형식으로 들어와서 202505 / 14 으로 형식 분리
        String workym = null;
        String workday = null;
        if (serchday != null && serchday.length() == 8) {
            workym = serchday.substring(0, 6);
            workday = serchday.substring(6, 8);
        }

        // 빈 문자열 허용을 위해 String 그대로 사용
        String divisionStr = (work_division != null) ? work_division : "";
        paramMap.addValue("work_division", divisionStr);

        String departStr = (depart != null) ? depart : "";
        paramMap.addValue("depart_id", departStr);

        paramMap.addValue("workym", workym);
        paramMap.addValue("workday", workday);
        paramMap.addValue("spjangcd", spjangcd);

        String sql = """
            SELECT
           ROW_NUMBER() OVER (ORDER BY CAST(s."Code" AS INTEGER)) AS row_num,
                t.workym,
                t.workday,
                SUBSTRING(t.workym, 1, 4) || '-' || SUBSTRING(t.workym, 5, 2) || '-' || LPAD(t.workday, 2, '0') AS workymd,
                p.id,
                t.worknum,
                t.holiyn,
                t.workyn,
                t.workcd,
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
                t.address,
                g."Value" AS group_name,
                s."Value" as jik_id,
                tp210.worknm as worknm,
                p."Name" as first_name,
                t.spjangcd as spjangcd
            FROM person p
            LEFT JOIN tb_pb201 t
             ON t.personid = p.id
             AND t.workym = :workym
             AND t.workday = :workday
           LEFT JOIN (
              SELECT "Code", "Value"
              FROM sys_code
               WHERE "CodeType" = 'work_division'
           ) g ON g."Code" = LPAD(p."PersonGroup_id"::text, 2, '0')
             LEFT JOIN (
                 SELECT "Code", "Value"
                 FROM sys_code
                 WHERE "CodeType" = 'jik_type'
             ) s ON s."Code" = p.jik_id
             LEFT JOIN tb_pb210 tp210 ON tp210.workcd = t.workcd
            WHERE(
               :work_division = '' OR
               LPAD(p."PersonGroup_id"::text, 2, '0') = :work_division
                )
                AND (
               :depart_id = '' OR
               LPAD(p."Depart_id"::text, 2, '0') = :depart_id
                )
              AND p.spjangcd =:spjangcd
              AND (
                  p.rtflag = '0'
                  OR (
                    p.rtflag != '0'
                    AND EXISTS (
                      SELECT 1
                      FROM tb_pb201 t2
                      WHERE t2.personid = p.id
                        AND t2.workym = :workym
                        AND t2.workday = :workday
                    )
                  )
                )
        """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        return items;
    }


    public List<Map<String, String>> workcdList(String spjangcd) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("spjangcd", spjangcd);


        String sql = """
                SELECT worknm, workcd
                FROM tb_pb210
                where spjangcd = :spjangcd
            """;
        // SQL 실행
        List<Map<String, Object>> rows = this.sqlRunner.getRows(sql, dicParam);

        List<Map<String, String>> result = rows.stream()
                .map(row -> {
                    Map<String, String> map = new HashMap<>();
                    map.put("worknm", (String) row.get("worknm"));
                    map.put("workcd", (String) row.get("workcd"));
                    return map;
                })
                .toList();

        return result;
    }


    /*월정산 save*/
    public int insertWorkSummary(String spjangcd, String workym) {
        String sql = """
        INSERT INTO tb_pb203 (
            workym,
            workday,
            personid,
            fixflag,
            worktime,
            nomaltime,
            overtime,
            nighttime,
            holitime,
            jitime,
            jotime,
            yuntime,
            abtime,
            bantime,
            spjangcd
        )
        SELECT
            ? AS workym,
            COUNT(*) AS workday,
            t.personid,
            0 AS fixflag,
            SUM(t.worktime),
            SUM(t.nomaltime),
            SUM(t.overtime),
            SUM(t.nighttime),
            SUM(t.holitime),
            SUM(t.jitime),
            SUM(t.jotime),
            SUM(t.yuntime),
            SUM(t.abtime),
            SUM(t.bantime),
            t.spjangcd
        FROM tb_pb201 t
        WHERE t.spjangcd = ?
          AND t.workym = ?
          AND t.fixflag ='1'
        GROUP BY t.personid, t.workym, t.spjangcd
        """;

        return jdbcTemplate.update(sql, workym, spjangcd, workym);
    }



/*read*/
    public List<Map<String, Object>> getMonthlyReadList(String person_name, String startdate, String spjangcd,String depart) {
        MapSqlParameterSource paramMap = new MapSqlParameterSource();

        String departStr = (depart != null) ? depart : "";
        paramMap.addValue("depart_id", departStr);

        String personStr = (person_name!= null) ? person_name : "";
        paramMap.addValue("person_name", personStr);


        paramMap.addValue("startdate", startdate);
        paramMap.addValue("spjangcd", spjangcd);

        String sql = """
            SELECT
            t.workym
            ,t.personid
            ,t.workday
            ,t.worktime
            ,t.nomaltime
            ,t.overtime
            ,t.nighttime
            ,t.holitime
            ,t.jitime
            ,t.jotime
            ,t.yuntime
            ,t.abtime
            ,t.bantime
            ,t.fixflag
            ,s."Value" as jik_id
            ,p."Name" as first_name
            ,t.spjangcd as spjangcd
            FROM tb_pb203 t
            LEFT JOIN person p ON p.id = t.personid
             LEFT JOIN (
                 SELECT "Code", "Value"
                 FROM sys_code
                 WHERE "CodeType" = 'jik_type'
             ) s ON s."Code" = p.jik_id
            WHERE
                (:person_name = '' OR t.personid::text = :person_name)
                AND (
               :depart_id = '' OR
               LPAD(p."Depart_id"::text, 2, '0') = :depart_id
                )
              AND t.spjangcd =:spjangcd
              AND t.workym = :startdate
              AND p.rtflag = '0'
        """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        return items;
    }

    // 사원별 월단위 일일근태기록 조회
    public List<Map<String,Object>> getPersonMonthDaily(
            String startdate,
            Integer personid,
            String spjangcd
    ){
        MapSqlParameterSource param = new MapSqlParameterSource();

        param.addValue("startdate", startdate);
        param.addValue("personid", personid);
        param.addValue("spjangcd", spjangcd);

        String sql = """ 
                WITH d AS (
                    SELECT
                        dt,
                        to_char(dt,'YYYYMM') workym,
                        to_char(dt,'DD') workday,
                        to_char(dt,'YY/MM/DD') workymd,
                        extract(isodow from dt) worknum,
                        extract(week from dt) week_no,
                        CASE extract(isodow from dt)
                            WHEN 1 THEN '월'
                            WHEN 2 THEN '화'
                            WHEN 3 THEN '수'
                            WHEN 4 THEN '목'
                            WHEN 5 THEN '금'
                            WHEN 6 THEN '토'
                            WHEN 7 THEN '일'
                        END weekday
                    FROM generate_series(
                        to_date(:startdate,'YYYYMM'),
                        to_date(:startdate,'YYYYMM') + interval '1 month - 1 day',
                        interval '1 day'
                    ) dt
                ),
                base AS (
                    SELECT
                        d.week_no,
                        d.dt,
                        d.workymd,
                        d.weekday,
                        d.worknum,
                        coalesce(t.nomaltime,0) nomaltime,
                        coalesce(t.overtime,0) overtime,
                        coalesce(t.holitime,0) holitime,
                        coalesce(t.nomaltime,0)
                        + coalesce(t.overtime,0)
                        + coalesce(t.holitime,0) total_time,
                        t.starttime,
                        t.endtime,
                        tp210.worknm,
                        t.remark,
                        t.fixflag,
                        1 sort_order
                    FROM d
                    LEFT JOIN tb_pb201 t
                        ON t.workym = d.workym
                        AND t.workday = d.workday
                        AND t.personid = :personid::int
                        AND t.spjangcd = :spjangcd
                    LEFT JOIN tb_pb210 tp210
                        ON tp210.workcd = t.workcd
                ),
                week_sum AS (
                    SELECT
                    week_no,
                    NULL::timestamp dt,
                    '주간근무시간' workymd,
                    NULL::varchar weekday,
                    NULL::int worknum,
                    NULL::numeric nomaltime,
                    NULL::numeric overtime,
                    NULL::numeric holitime,
                    SUM(total_time) total_time,
                    NULL::varchar starttime,
                    NULL::varchar endtime,
                    NULL::varchar worknm,
                    NULL::varchar remark,
                    NULL::varchar fixflag,
                    2 sort_order
                    FROM base
                    GROUP BY week_no
                )
                SELECT *
                FROM (
                    SELECT * FROM base
                    UNION ALL
                    SELECT * FROM week_sum
                ) x
                ORDER BY week_no, dt NULLS LAST, sort_order;
                """;
        return sqlRunner.getRows(sql, param);
    }

}
