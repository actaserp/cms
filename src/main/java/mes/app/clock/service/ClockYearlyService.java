package mes.app.clock.service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class ClockYearlyService {

    @Autowired
    SqlRunner sqlRunner;

    //read
    public List<Map<String, Object>> getYearlyList(String year,String name, String spjangcd,String startdate,String rtflag) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("year", year);
        dicParam.addValue("spjangcd", spjangcd);
        dicParam.addValue("startdate", startdate);

        String sql = """
                SELECT
                    ROW_NUMBER() OVER (ORDER BY p.jik_id) AS rownum,
                    p.id,
                    p."Name" AS person_name,
                    s."Value" AS jik_id,
                    p.rtdate AS rtdate,
                    (COALESCE(tb209.ewolnum, 0) + COALESCE(tb209.holinum, 0) - COALESCE(tb204.daynum, 0)) AS restnum,
                    tb209.ewolnum,
                    tb209.holinum,
                    tb204.daynum
                FROM person p
                LEFT JOIN (
                    SELECT "Code", "Value"
                    FROM sys_code
                    WHERE "CodeType" = 'jik_type'
                ) s ON s."Code" = p.jik_id
                LEFT JOIN (
                    SELECT personid, SUM(daynum) AS daynum
                    FROM tb_pb204
                    where fixflag = '1'
                    GROUP BY personid
                ) tb204 ON p.id = tb204.personid
                LEFT JOIN (
                    SELECT t.*
                    FROM tb_pb209 t
                    INNER JOIN (
                        SELECT personid, MAX(reqdate) AS max_reqdate
                        FROM tb_pb209
                        GROUP BY personid
                    ) latest ON t.personid = latest.personid AND t.reqdate = latest.max_reqdate
                ) tb209 ON p.id = tb209.personid
                WHERE 1 = 1
                """;

        if (rtflag != null && !rtflag.isEmpty()) {
            sql += " and rtflag = :rtflag ";
            dicParam.addValue("rtflag",  rtflag);
        }

        if (name != null && !name.isEmpty()) {
            sql += " AND p.\"Name\" LIKE concat('%', :name, '%') ";
            dicParam.addValue("name", "%" + name + "%");
        }
        sql  +="""
            GROUP BY
                p.id, s."Value", tb209.ewolnum, tb209.holinum, tb204.daynum, p."Name", p.rtdate, p.jik_id
            ORDER BY p.jik_id
            """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        return items;
    }

//연차생성
    public List<Map<String, Object>> YearlyCreate(String year,String spjangcd,String startdate, String name) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("year", year);
        dicParam.addValue("spjangcd", spjangcd);
        dicParam.addValue("startdate", startdate);

        String personStr = (name!= null) ? name : "";
        dicParam.addValue("name", personStr);

        String sql = """
                WITH base AS (
                    SELECT
                        p.spjangcd,
                        p."Name" AS person_name,
                        TO_DATE(p.rtdate, 'YYYYMMDD') AS rtdate,
                        p.id,
                        TO_DATE(:year::INT - 1 || '-12-31', 'YYYY-MM-DD') AS end_date,
                        TO_DATE(:startdate, 'YYYYMMDD') AS start_date,
                         ROW_NUMBER() OVER (ORDER BY TO_DATE(p.rtdate, 'YYYYMMDD')) AS rownum ,
                         p.jik_id
                    FROM person p
                    WHERE TO_DATE(p.rtdate, 'YYYYMMDD') <= TO_DATE(:year::INT || '1231', 'YYYYMMDD')
                ),
                month_calc AS (
                    SELECT *,
                        CASE
                            WHEN end_date >= rtdate THEN
                                (DATE_PART('year', AGE(end_date, rtdate)) * 12 +
                                 DATE_PART('month', AGE(end_date, rtdate)))
                            ELSE 0
                        END AS llMonth
                    FROM base
                ),
                holiday_calc AS (
                    SELECT *,
                        CASE
                            WHEN llMonth < 12 THEN 0
                            WHEN llMonth = 12 THEN 1
                            WHEN llMonth <= 24 THEN 15
                            WHEN llMonth <= 48 THEN 16
                            WHEN llMonth <= 72 THEN 17
                            WHEN llMonth <= 96 THEN 18
                            WHEN llMonth <= 120 THEN 19
                            WHEN llMonth <= 144 THEN 20
                            WHEN llMonth <= 168 THEN 21
                            WHEN llMonth <= 192 THEN 22
                            WHEN llMonth <= 216 THEN 23
                            WHEN llMonth <= 240 THEN 24
                            ELSE
                                CASE
                                    WHEN FLOOR((llMonth / 12.0) * 15) + 0.5 < ((llMonth / 12.0) * 15)
                                    THEN FLOOR((llMonth / 12.0) * 15) + 0.5
                                    ELSE FLOOR((llMonth / 12.0) * 15)
                                END
                        END AS llHoliynum
                    FROM month_calc
                )
                SELECT
                    h.spjangcd,
                    h.id,
                    h.person_name,
                    h.rtdate,
                    h.end_date,
                    h.llMonth,
                    h.llHoliynum AS holinum,
                    COALESCE(l.cnt, 0) AS ewolnum,
                    (h.llHoliynum + COALESCE(l.cnt, 0)) AS restnum,
                    h.rownum,
                    s."Value" AS jik_id
                FROM holiday_calc h
                LEFT JOIN (
                    SELECT "Code", "Value"
                    FROM sys_code
                    WHERE "CodeType" = 'jik_type'
                ) s ON s."Code" = h.jik_id
                LEFT JOIN LATERAL (
                    SELECT restnum AS cnt
                    FROM TB_PB209
                    WHERE spjangcd = 'ZZ'
                      AND personid = h.id
                      AND SUBSTRING(reqdate, 1, 4)::INT = (:year)::INT - 1
                    ORDER BY reqdate DESC
                    LIMIT 1
                ) l ON TRUE
                WHERE h.spjangcd = :spjangcd
                ORDER BY h.rtdate
                """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        return items;
    }


// 월차생성
    public List<Map<String, Object>> MonthlyCreate(String year, String spjangcd, String startdate, String name) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("year", year);
        dicParam.addValue("spjangcd", spjangcd);

        dicParam.addValue("startdate", startdate);


        String personStr = (name != null) ? name : "";
        dicParam.addValue("name", personStr);

        String sql = """
                WITH base AS (
                    SELECT
                        p.id AS personid,
                        p.spjangcd,
                        p."Name" AS person_name,
                        TO_DATE(p.rtdate, 'YYYYMMDD') AS rtdate,
                        CASE
                            WHEN TO_CHAR(CURRENT_DATE, 'YYYYMM') = :startdate
                                THEN CURRENT_DATE
                            ELSE (DATE_TRUNC('month', TO_DATE(:startdate || '01', 'YYYYMMDD')) + INTERVAL '1 month - 1 day')::DATE
                        END AS nowdate
                    FROM person p
                    WHERE TO_DATE(p.rtdate, 'YYYYMMDD') <= TO_DATE('20251231', 'YYYYMMDD')
                ),
                month_list AS (
                    SELECT
                        gs.month_start::DATE AS month_start,
                        TO_CHAR(gs.month_start, 'YYYYMM') AS ym
                    FROM generate_series(
                        (SELECT MIN(TO_DATE(p.rtdate, 'YYYYMMDD')) FROM person p),
                        DATE_TRUNC('month', TO_DATE(:startdate, 'YYYYMMDD')),
                        INTERVAL '1 month'
                    ) AS gs(month_start)
                ),
                attendance_summary AS (
                    SELECT
                        pb.personid,
                        TO_CHAR(TO_DATE(pb.workym, 'YYYYMMDD'), 'YYYYMM') AS ym,
                        MAX(COALESCE(pb.jitime, 0) + COALESCE(pb.jotime, 0) + COALESCE(pb.abtime, 0)) AS bad_record
                    FROM tb_pb203 pb
                    GROUP BY pb.personid, TO_CHAR(TO_DATE(pb.workym, 'YYYYMMDD'), 'YYYYMM')
                ),
                valid_months AS (
                    SELECT
                        b.personid,
                        COUNT(*) AS valid_month_count
                    FROM base b
                    CROSS JOIN month_list ml
                    LEFT JOIN attendance_summary a ON a.personid = b.personid AND a.ym = ml.ym
                    WHERE (a.bad_record IS NULL OR a.bad_record = 0)
                      AND ml.month_start >= b.rtdate
                      AND ml.month_start <= b.nowdate
                    GROUP BY b.personid
                ),
                leave_used AS (
                     SELECT
                         personid,
                         COALESCE(SUM(daynum), 0) AS used_days
                     FROM tb_pb204
                     WHERE TO_CHAR(TO_DATE(reqdate, 'YYYYMMDD'), 'YYYY') = TO_CHAR(TO_DATE(:year || '0101', 'YYYYMMDD') - INTERVAL '1 year', 'YYYY')
                     GROUP BY personid
                 ),
                final_calc AS (
                    SELECT
                        b.spjangcd,
                        b.personid,
                        b.person_name,
                        b.rtdate,
                        b.nowdate,
                        COALESCE(vm.valid_month_count, 0) AS valid_month_count,
                        LEAST(COALESCE(vm.valid_month_count, 0), 15) AS holinum,
                        COALESCE(lu.used_days, 0) AS daynum,
                        LEAST(COALESCE(vm.valid_month_count, 0), 15) - COALESCE(lu.used_days, 0) AS restnum,
                        AGE(b.nowdate, b.rtdate) AS duration
                    FROM base b
                    LEFT JOIN valid_months vm ON b.personid = vm.personid
                    LEFT JOIN leave_used lu ON b.personid = lu.personid
                )
                SELECT
                    f.spjangcd,
                    f.personid AS id,
                    f.person_name,
                    f.rtdate,
                    f.nowdate,
                    f.valid_month_count AS no_month_count,
                    f.holinum,
                    f.daynum,
                    f.restnum
                FROM final_calc f
                WHERE f.spjangcd = 'ZZ'
                  AND f.duration < INTERVAL '1 year'
                ORDER BY f.personid;
                """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
    }


    public List<Map<String, Object>> getYearlyDetail(Integer id, String year){

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("id", id);
        dicParam.addValue("year", year);

        String sql = """
                WITH latest_pb209 AS (
                    SELECT
                        tb.reqdate,
                        tb.personid,
                        tb.ewolnum,
                        tb.holinum,
                        tb.restnum
                    FROM tb_pb209 tb
                    JOIN (
                        SELECT personid, MAX(reqdate) AS max_reqdate
                        FROM tb_pb209
                        WHERE personid = :id
                          AND LEFT(CAST(reqdate AS VARCHAR), 4) = :year
                        GROUP BY personid
                    ) latest ON tb.personid = latest.personid AND tb.reqdate = latest.max_reqdate
                ),
                pb204_with_running_total AS (
                    SELECT
                        t.reqdate,
                        t.personid,
                        t.frdate,
                        t.todate,
                        t.daynum,
                        t.workcd,
                        w.worknm,
                        SUM(COALESCE(t.daynum, 0)) OVER (
                            ORDER BY t.reqdate
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS cum_daynum
                    FROM tb_pb204 t
                    LEFT JOIN tb_pb210 w ON t.workcd = w.workcd 
                    WHERE t.personid = :id
                      AND t.fixflag = '1'
                      AND LEFT(CAST(t.reqdate AS VARCHAR), 4) = :year
                ),
                unioned_data AS (
                    SELECT
                        reqdate,
                        personid,
                        NULL AS frdate,
                        NULL AS todate,
                        NULL AS daynum,
                        NULL AS workcd,
                        '생성' AS worknm,
                        ewolnum,
                        holinum,
                        restnum
                    FROM latest_pb209
                               
                    UNION ALL
                               
                    SELECT
                        p.reqdate,
                        p.personid,
                        p.frdate,
                        p.todate,
                        p.daynum,
                        p.workcd,
                        p.worknm,
                        0.00 AS ewolnum,
                        0.00 AS holinum,
                        GREATEST(l.restnum - p.cum_daynum) AS restnum
                    FROM pb204_with_running_total p
                    CROSS JOIN latest_pb209 l
                )
                SELECT
                    ROW_NUMBER() OVER (ORDER BY reqdate) - 1 AS rownum,
                    *
                FROM unioned_data
                ORDER BY reqdate;
                """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);


        return items;
    }


}
