package mes.app.summary.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.Map;
import mes.domain.services.SqlRunner;

@Service
public class SujuMonthSummarySerivce {
	
	@Autowired
	SqlRunner sqlRunner;

	public List<Map<String, Object>> getList(String cboYear,Integer cboCompany,Integer cboMatGrp,String cboDataDiv,String spjangcd) {
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
			data_column = " s.\"SujuQty\" "; 
		}else {
			data_column = " s.\"Price\" + coalesce(s.\"Vat\",0) ";
		}
		
		
		String sql ="""
				with A as (
	            select s."Material_id" as mat_pk, s."CompanyName" as company_name
	            , extract (month from s."JumunDate") as data_month
	            , sum(s."SujuQty") as qty_sum
	            , sum(s."Price" + coalesce(s."Vat", 0)) as money_sum
	            """;
		
		sql += " ,sum( " + data_column + " ) as suju_sum "; 
		
		sql +="""
	            from suju s
                inner join material m on m.id = s."Material_id"
	            where s."JumunDate" between cast(:date_form as date) and cast(:date_to as date)
	            and s.spjangcd = :spjangcd
				""";
		if(cboCompany != null) {
			sql += """
					and s."Company_id" = :cboCompany
					""";
		}
		
		if(cboMatGrp != null) {
			sql += """
					 and m."MaterialGroup_id" = :cboMatGrp
					""";
		}
		
		sql += """
				group by s."Material_id", s."CompanyName", extract (month from s."JumunDate")
                )
	            select 1 as grp_idx, mg."Name" as mat_grp_name, m."Code" as mat_code, m."Name" as mat_name, A.mat_pk
                , u."Name" as unit_name
	            , A.company_name
			    , sum(A.qty_sum) as year_qty_sum	        
			    , sum(A.money_sum) as year_money_sum
				""";
		
		for(int i=1; i<13; i++) {
			sql+=", min(case when A.data_month = " + i + " then A.suju_sum end) as mon_"+i+" ";
		}
			
		sql+="""
				from A 
        inner join material m on m.id = A.mat_pk
        left join unit u on u.id = m."Unit_id"
        left join mat_grp mg on mg.id = m."MaterialGroup_id"
        group by mg."Name", m."Code", m."Name", A.mat_pk, u."Name", A.company_name
        --order by m."Code", m."Name", A.company_name
        union all 
        select 2 as grp_idx, mg."Name" as mat_grp_name, m."Code" as mat_code, m."Name" as mat_name, A.mat_pk
        , u."Name" as unit_name
        , '소계' as company_name
		, sum(A.qty_sum) as year_qty_sum	 
		, sum(A.money_sum) as year_money_sum
				""";
		
		
		for(int i=1; i<13; i++) {
			sql += ", sum(case when A.data_month = "+ i +" then A.suju_sum end) as mon_"+i+" ";
		}
			
		sql += """
				from A 
        inner join material m on m.id = A.mat_pk
        left join unit u on u.id = m."Unit_id"
        left join mat_grp mg on mg.id = m."MaterialGroup_id"
        group by mg."Name", m."Code", m."Name", A.mat_pk, u."Name"
        order by mat_code, mat_name, grp_idx, company_name
				""";
		
		
		
		
		
		

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

}
