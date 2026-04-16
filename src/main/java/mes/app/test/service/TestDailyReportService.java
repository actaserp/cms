package mes.app.test.service;

import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class TestDailyReportService {

  @Autowired
  SqlRunner sqlRunner;

  public List<Map<String, Object>> getList(Integer workCenterId, String start ,String end ) {
    MapSqlParameterSource dicParam = new MapSqlParameterSource();
    dicParam.addValue("work_id", workCenterId);
    dicParam.addValue("date_from", start);
    dicParam.addValue("date_to", end);
    String sql = """
        select 
        dt."Name" AS defect_type,
        i."DefectType_id" as defect_pk,
        i."InspectionQty" as inspection_qty,
        i."DefectQty" as defect_qty,
        i."InspectionDate" 
        from inspection_reports i
        JOIN defect_type dt ON dt.id = i."DefectType_id"
        where "InspectionDate" BETWEEN CAST(:date_from AS date) AND CAST(:date_to AS date)
        and i."WorkCenter_id" =:work_id
        """;

//    log.info("검사일보 read SQL: {}", sql);
//    log.info("SQL Parameters: {}", dicParam.getValues());
  return  sqlRunner.getRows(sql, dicParam);
  }

  public Map<String, Object> getDetail(Integer work_center_id, String search_date, Integer defect_pk) {
    MapSqlParameterSource dicParam = new MapSqlParameterSource();
    dicParam.addValue("work_center_id", work_center_id);
    dicParam.addValue("search_date", search_date);
    dicParam.addValue("defect_pk", defect_pk);

    String sql= """
        select
        jr."WorkOrderNumber",
        jr."ProductionDate" ,
        dt.id AS defect_pk,
        dt."Name" AS defect_type,
         SUM(COALESCE(jr."GoodQty", 0) + COALESCE(jr."DefectQty", 0)) AS total_prod_qty,
        jrd."DefectQty" AS defect_qty,
         jr."WorkCenter_id"
        FROM job_res jr
        JOIN job_res_defect jrd ON jrd."JobResponse_id" = jr.id
        JOIN defect_type dt ON dt.id = jrd."DefectType_id"
        where jrd."DefectType_id" = :defect_pk
        and jr."ProductionDate" = cast(:search_date as date)
        and jr."WorkCenter_id" =:work_center_id
        GROUP BY
        jr."WorkCenter_id", jr."WorkOrderNumber",
        jr."ProductionDate", jrd."DefectQty",
        dt.id,  dt."Name"
        """;

//    log.info("검사일보 detail SQL: {}", sql);
//    log.info("SQL Parameters: {}", dicParam.getValues());
    return sqlRunner.getRow(sql, dicParam);
  }

  public List<Map<String, Object>> defectsList(Integer work_id) {
    MapSqlParameterSource dicParam = new MapSqlParameterSource();
    dicParam.addValue("work_id", work_id);
      String sql= """
        select
        dt.id as defect_type_id ,
        dt."Name" as defect_type
        from proc_defect_type pdt
        left join work_center wc on wc.id = pdt."Process_id"
        left join defect_type dt on pdt."DefectType_id" = dt.id
        where wc.id = :work_id
      """;
    return sqlRunner.getRows(sql, dicParam);
  }

  public List<Map<String, Object>> findInspectionQty(String date) {
    MapSqlParameterSource params = new MapSqlParameterSource()
        .addValue("inspection_date", date); // 'YYYY-MM-DD' 가정

    String sql = """
      select 
        ir."DefectType_id" ,
        ir."DefectQty" ,
        ir."InspectionQty" as inspection_qty
      from inspection_reports ir
      where ir."InspectionDate" = to_date(:inspection_date, 'YYYY-MM-DD')
      order by "InspectionQty"
    """;

    return sqlRunner.getRows(sql, params);
  }

  public List<Map<String, Object>> defectsWithQty(Integer workId, String date) {
    // 1) 작업장 기준 불량유형 목록
    List<Map<String, Object>> defects = defectsList(workId);

    // 2) 날짜(필요시 작업장까지) 기준 저장값 조회
    List<Map<String, Object>> rows = findInspectionQty(date); // 현재 시그니처 유지

    // 3) DefectType_id -> 조회행 매핑
    Map<Integer, Map<String, Object>> byType = new HashMap<>();
    if (rows != null) {
      for (Map<String, Object> r : rows) {
        Object key = r.get("DefectType_id");
        if (key instanceof Number) {
          byType.put(((Number) key).intValue(), r);
        }
      }
    }

    // 4) 매칭해서 값 주입 (없으면 inspection_qty=null, defect_qty=0)
    for (Map<String, Object> d : defects) {
      Integer typeId = ((Number) d.get("defect_type_id")).intValue();
      Map<String, Object> r = byType.get(typeId);

      d.put("inspection_qty", r != null ? r.get("inspection_qty") : 0);
      d.put("defect_qty",     r != null ? r.get("DefectQty")      : 0);
    }

    return defects;
  }

  @Transactional
  public void saveReplacing(Integer workCenterId,
                            LocalDate inspectionDate,
                            Double inspectionQty,
                            String spjangcd,
                            Integer createrId,
                            List<Map<String, Object>> lines) {
    // 1) 삭제
    var del = new MapSqlParameterSource()
        .addValue("work_center_id", workCenterId)
        .addValue("inspection_date", java.sql.Date.valueOf(inspectionDate));
    String delSql = """
      delete from inspection_reports
      where "WorkCenter_id" = :work_center_id
      and "InspectionDate" = :inspection_date
    """;
//    log.info("검사일보  del SQL: {}", delSql);
//    log.info("SQL Parameters: {}", del.getValues());
    sqlRunner.execute(delSql, del);

    // 2) 자장
    String insSql = """
      insert into inspection_reports
      (_created, _creater_id, "ProcessOrder", "LotIndex",
       "DefectQty", "DefectType_id", spjangcd,
       "InspectionDate", "WorkCenter_id", "InspectionQty")
      values (now(), :creater_id, :process_order, :lot_index,
              :defect_qty, :defect_type_id, :spjangcd,
              :inspection_date, :work_center_id, :inspection_qty)
    """;

    short processOrder = 0, lotIndex = 0;

    // inspectionQty가 null이면 0으로
    double headerQty = inspectionQty == null ? 0d : inspectionQty;

    // (선택) 중복 defectTypeId 병합 방지: 같은 타입이 여러번 오면 합산
    // 필요 없으면 이 블록 제거하고 바로 for문으로 진행
    Map<Integer, Double> merged = new LinkedHashMap<>();
    for (var line : lines) {
      Integer typeId = toInteger(line.get("defectTypeId"));
      if (typeId == null) continue; // 타입 없는 행만 스킵

      Double q = toDouble(line.get("defectQty"));
      double qty = (q == null || q < 0) ? 0d : q; // null/음수 → 0으로 저장
      merged.merge(typeId, qty, Double::sum);
    }

    for (var entry : merged.entrySet()) {
      var p = new MapSqlParameterSource()
          .addValue("creater_id", createrId)
          .addValue("process_order", processOrder)
          .addValue("lot_index", lotIndex)
          .addValue("defect_qty", entry.getValue())          // 0도 그대로 저장
          .addValue("defect_type_id", entry.getKey())
          .addValue("spjangcd", spjangcd)
          .addValue("inspection_date", java.sql.Date.valueOf(inspectionDate))
          .addValue("work_center_id", workCenterId)
          .addValue("inspection_qty", headerQty);
      sqlRunner.execute(insSql, p);
    }
  }
  private Double toDouble(Object o) {
    if (o == null) return null;
    if (o instanceof Number n) return n.doubleValue();
    String s = o.toString().trim().replace(",", "");
    return s.isEmpty() ? null : Double.valueOf(s);
  }
  private Integer toInteger(Object o) {
    if (o == null) return null;
    if (o instanceof Number n) return n.intValue();
    String s = o.toString().trim();
    return s.isEmpty() ? null : Integer.valueOf(s);
  }
}
