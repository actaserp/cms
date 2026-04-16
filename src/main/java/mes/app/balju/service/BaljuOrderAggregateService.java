package mes.app.balju.service;

import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class BaljuOrderAggregateService {
  @Autowired
  SqlRunner sqlRunner;

  public List<Map<String, Object>> getList(String srchStartDt, String srchEndDt, Integer cboCompany, Integer cboMatGrp, String cBaljuState, String spjangcd) {
    MapSqlParameterSource paramMap = new MapSqlParameterSource();
    paramMap.addValue("srchStartDt", srchStartDt);
    paramMap.addValue("srchEndDt", srchEndDt);
    paramMap.addValue("cboCompany", cboCompany);
    paramMap.addValue("cboMatGrp", cboMatGrp);
    paramMap.addValue("cBaljuState", cBaljuState);
    paramMap.addValue("spjangcd", spjangcd);

    String sql = """
         with A as (
             select b."Material_id" as mat_pk,
             b."CompanyName" as company_name,
             sum(b."SujuQty") as suju_sum,
             sum(b."Price") as price_sum, -- 공급가 합계
             sum(b."Price" + coalesce(b."Vat", 0)) as amount_sum,
             sum(b."Vat") as vat_sum
             from balju b
                inner join material m on m.id = b."Material_id" and m.spjangcd = b.spjangcd
             where b."JumunDate" between cast(:srchStartDt as date) 
             and cast(:srchEndDt as date) 
             and b.spjangcd = :spjangcd
        """;

    if (cboCompany != null) {
      sql += """ 
          and b."Company_id" = :cboCompany
          """;
    }

    if (cboMatGrp != null) {
      sql += """
          and m."MaterialGroup_id" = :cboMatGrp
          """;
    }
    if (cBaljuState != null && !cBaljuState.isBlank()) {
      sql += """
      and b."State" = :cBaljuState
  """;
    }

    sql += """
        group by b."Material_id", b."CompanyName" 
                )
             select mg."Name" as mat_grp_name, m."Code" as mat_code, m."Name" as mat_name, A.mat_pk
                , u."Name" as unit_name,
                 sum(A.suju_sum) over(partition by A.mat_pk, A.company_name) as tot_suju_sum,
                 sum(A.price_sum) over(partition by A.mat_pk,  A.company_name) as tot_price_sum,
                 sum(A.vat_sum)over(partition by A.mat_pk,  A.company_name) as tot_vat_sum,
                 sum(A.amount_sum)over(partition by A.mat_pk,  A.company_name) as amount_sum,
                 A.company_name,
                 A.suju_sum,
                 A.price_sum
             from A 
             inner join material m on m.id = A.mat_pk
                left join mat_grp mg on mg.id = m."MaterialGroup_id" and mg.spjangcd = m.spjangcd
                left join unit u on u.id = m."Unit_id" and u.spjangcd = m.spjangcd
        """;

    List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
//    log.info("발주량집계 read SQL: {}", sql);
//    log.info("SQL Parameters: {}", paramMap.getValues());
    return items;
  }
}

