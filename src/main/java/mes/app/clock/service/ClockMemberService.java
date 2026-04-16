package mes.app.clock.service;

import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class ClockMemberService {

    @Autowired
    SqlRunner sqlRunner;

    public List<Map<String, Object>> getMemberList(String start_date,String end_date,String person_name,String spjangcd) {
        MapSqlParameterSource paramMap = new MapSqlParameterSource();

        paramMap.addValue("start_date", start_date);
        paramMap.addValue("end_date", end_date);
        paramMap.addValue("spjangcd", spjangcd);

        String personStr = (person_name!= null) ? person_name : "";
        paramMap.addValue("person_name", personStr);
        String sql = """
          SELECT
              t.id as id,
              t.spjangcd as spjangcd,
              t.reqdate as reqdate,
              t.personid as personid,
              t.frdate as frdate,
              t.todate as todate,
              t.sttime as sttime,
              t.edtime as edtime,
              t.daynum as daynum,
              t.workcd as workcd,
              t.remark as remark,
              t.fixflag as fixflag,
              tb210.yearflag as yearflag,
              tb210.worknm as worknm,
              p."Name" as first_name,
              s."Value" as jik_id,
              sc."Value" as appgubunnm
          from tb_pb204 t
            LEFT JOIN person p ON p.id = t.personid
            LEFT JOIN (
                 SELECT "Code", "Value"
                 FROM sys_code
                 WHERE "CodeType" = 'jik_type'
             ) s ON s."Code" = p.jik_id
             LEFT JOIN (
                 SELECT "Code", "Value"
                 FROM sys_code
                 WHERE "CodeType" = 'approval_status'
             ) sc ON sc."Code" = t.appgubun
             LEFT JOIN tb_pb210 tb210 ON tb210.workcd = t.workcd
          WHERE t.reqdate between :start_date and :end_date
          AND t.spjangcd = :spjangcd
          AND (:person_name = '' OR t.personid::text = :person_name)
          order by reqdate
        """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        return items;
    }

}
