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
public class BaljuOrderListService {
  @Autowired
  SqlRunner sqlRunner;

  public List<Map<String, Object>> getList(String cboYear, Integer cboCompany, Integer cboMatGrp, String cboDataDiv, String spjangcd) {
    MapSqlParameterSource paramMap = new MapSqlParameterSource();
    paramMap.addValue("cboYear", cboYear);
    paramMap.addValue("cboCompany", cboCompany);
    paramMap.addValue("cboMatGrp", cboMatGrp);
    paramMap.addValue("cboDataDiv", cboDataDiv);
    paramMap.addValue("spjangcd", spjangcd);

    String data_column = "";

    String data_year = cboYear;

    paramMap.addValue("date_form",data_year+"-01-01" );
    paramMap.addValue("date_to",data_year+"-12-31" );

    if(cboDataDiv.equals("qty")) {
      data_column = " b.\"SujuQty\" ";
    }else {
      data_column = " b.\"Price\" + coalesce(b.\"Vat\",0) ";
    }

    String sql ="""
				with A as (
	            select b."Material_id" as mat_pk, b."CompanyName" as company_name
	            , extract (month from b."JumunDate") as data_month
	            , sum(b."SujuQty") as qty_sum
	            , sum(b."Price"+ coalesce(b."Vat", 0)) as money_sum
	            """;

    sql += " ,sum( " + data_column + " ) as balju_sum ";

    sql +="""
	            from balju b
                inner join material m on m.id = b."Material_id" and m.spjangcd = b.spjangcd
	            where b."JumunDate" between cast(:date_form as date) and cast(:date_to as date)
	             and b.spjangcd = :spjangcd
				""";
    if(cboCompany != null) {
      sql += """
					and b."Company_id" = :cboCompany
					""";
    }

    if(cboMatGrp != null) {
      sql += """
					 and m."MaterialGroup_id" = :cboMatGrp
					""";
    }

    sql += """
				group by b."Material_id", b."CompanyName", extract (month from b."JumunDate")
                )
	            select 1 as grp_idx, mg."Name" as mat_grp_name, m."Code" as mat_code, m."Name" as mat_name, A.mat_pk
                , u."Name" as unit_name
	            , A.company_name
			    , sum(A.qty_sum) as year_qty_sum	 
			    , sum(A.money_sum) as year_money_sum
				""";

    for(int i=1; i<13; i++) {
      sql+=", min(case when A.data_month = " + i + " then A.balju_sum end) as mon_"+i+" ";
    }

    sql+="""
				from A 
        inner join material m on m.id = A.mat_pk 
        left join unit u on u.id = m."Unit_id" and u.spjangcd = m.spjangcd
        left join mat_grp mg on mg.id = m."MaterialGroup_id" and mg.spjangcd = m.spjangcd
        group by mg."Name", m."Code", m."Name", A.mat_pk, u."Name", A.company_name
        --order by m."Code", m."Name", A.company_name
        union all 
        select 2 as grp_idx, mg."Name" as mat_grp_name, m."Code" as mat_code, m."Name" as mat_name, A.mat_pk
        , u."Name" as unit_name
        , '전체' as company_name
		, sum(A.qty_sum) as year_qty_sum	 
		, sum(A.money_sum) as year_money_sum
				""";


    for(int i=1; i<13; i++) {
      sql += ", sum(case when A.data_month = "+ i +" then A.balju_sum end) as mon_"+i+" ";
    }

    sql += """
				from A 
        inner join material m on m.id = A.mat_pk
        left join unit u on u.id = m."Unit_id" and u.spjangcd = m.spjangcd
        left join mat_grp mg on mg.id = m."MaterialGroup_id" and mg.spjangcd = m.spjangcd
        group by mg."Name", m."Code", m."Name", A.mat_pk, u."Name"
        order by mat_code, mat_name, grp_idx, company_name
				""";

//    log.info("월별 발주량 read SQL: {}", sql);
//    log.info("SQL Parameters: {}", paramMap.getValues());
    List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

    return items;
  }

}
