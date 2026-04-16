package mes.app.production.service;

import io.micrometer.core.instrument.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class ProdPlanServicr {


  @Autowired
  SqlRunner sqlRunner;

  // 수주 목록 조회
  public List<Map<String, Object>> getSujuList(String date_kind, String start, String end, Integer mat_group, String mat_name, String not_flag, String spjangcd) {

    MapSqlParameterSource dicParam = new MapSqlParameterSource();
    dicParam.addValue("start", Timestamp.valueOf(start + " 00:00:00"));
    dicParam.addValue("end", Timestamp.valueOf(end + " 23:59:59"));
    dicParam.addValue("mat_group", mat_group);
    dicParam.addValue("mat_name", mat_name);
    dicParam.addValue("spjangcd", spjangcd);

    if (StringUtils.isEmpty(date_kind)) {
      date_kind = "sales";
    }

    // 수주에서 수주량-예약량 = 수주량2(필요량)
    String sql = """
        with s as (
               select s.id, s."JumunDate", s."DueDate", s."JumunNumber"
               , s."CompanyName"
               , s."Material_id"
               , mg."Name" as "MaterialGroupName"
               , mg.id as "MaterialGroup_id"
               , m."Code" as mat_code
               , m."WorkCenter_id" as workcenter_id
               , m."Name" as mat_name
               , u."Name" as unit_name
               , s."SujuQty"
               , s."SujuQty2"
               , coalesce (s."ReservationStock",0) as "ReservationStock"
               , fn_code_name('suju_state', s."State") as "StateName"
               , fn_code_name('mat_type', mg."MaterialType") as mat_type_name
               , s."State"
               , s."Description" as description
               from suju s
               inner join material m on m.id = s."Material_id"
               inner join mat_grp mg on mg.id = m."MaterialGroup_id"
               left join unit u on m."Unit_id" = u.id
               where 1 = 1 and mg."MaterialType"!='sangpum'
               and s.spjangcd = :spjangcd
               and s.confirm = '0'
        """;

    if ("suju_date".equals(date_kind)) {
      sql += " and s.\"JumunDate\" between :start and :end ";
    } else {
      sql += " and s.\"DueDate\" between :start and :end ";
    }

    if (mat_group != null) {
      sql += " and mg.id = :mat_group ";
    }

    if (StringUtils.isEmpty(mat_name) == false) {
      sql += """
          and ( upper(m."Name") like concat('%%',upper(:mat_name),'%%')
                or upper(m."Code") = upper(:mat_name)
                )
          """;
    }

    sql += """
        )
       , q as (
           select s.id as suju_id
           , sum(jr."OrderQty") as ordered_qty
           from job_res jr
           inner join s on s.id = jr."SourceDataPk"
           and jr."SourceTableName"='suju'
           and jr."Material_id" = s."Material_id"
           where jr."State" <>'canceled'
           group by s.id
       )
       select s.id
       , s."JumunNumber"
       , to_char(s."JumunDate", 'yyyy-mm-dd') as "JumunDate"
       , to_char(s."DueDate", 'yyyy-mm-dd') as "DueDate"
       , s."CompanyName"
       , s.mat_type_name
       , s."MaterialGroupName"
       , s.mat_code
       , s.mat_name
       , s.unit_name
       , s."Material_id" as mat_pk
       , s."SujuQty" as "SujuQty"
       , s.description
       , s."StateName", s."State"
       from s
       left join q on q.suju_id = s.id
       where 1 = 1
        """;

    if (StringUtils.isEmpty(not_flag) == false) {
      sql += "  and (s.\"SujuQty2\"- coalesce (q.ordered_qty,0)) > 0 ";
    }

    if ("suju_date".equals(date_kind)) {
      sql += " order by s.\"JumunDate\" desc, s.\"JumunNumber\" ASC ";
    } else {
      sql += " order by s.\"JumunDate\"desc, s.\"JumunNumber\" ASC";
    }
//    log.info("작업계획등록 read SQL: {}", sql);
//    log.info("SQL Parameters: {}", dicParam.getValues());
    List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

    return items;
  }

}
