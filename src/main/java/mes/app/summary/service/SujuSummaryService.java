package mes.app.summary.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class SujuSummaryService {
	
	@Autowired
	SqlRunner sqlRunner;

	public List<Map<String, Object>> getList(String srchStartDt, String srchEndDt, Integer cboCompany,
			Integer cboMatGrp, String sujuState, String spjangcd) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("srchStartDt", srchStartDt);
		paramMap.addValue("srchEndDt", srchEndDt);
		paramMap.addValue("cboCompany", cboCompany);
		paramMap.addValue("cboMatGrp", cboMatGrp);
		paramMap.addValue("sujuState", sujuState);
		paramMap.addValue("spjangcd", spjangcd);
		
		
		String sql ="""
				 with A as (
	        select s."Material_id" as mat_pk, s."CompanyName" as company_name
	        , sum(s."SujuQty") as suju_sum
	        , sum(s."Price" + coalesce(s."Vat", 0)) as price_sum
	        from suju s
            inner join material m on m.id = s."Material_id"
	        where s."JumunDate" between cast(:srchStartDt as date) and cast(:srchEndDt as date)
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

		if(sujuState != null && !sujuState.trim().isEmpty()) {
			sql += """
					and s."State" = :sujuState
					""";
		}
		
		sql += """
				group by s."Material_id", s."CompanyName" 
            )
	        select mg."Name" as mat_grp_name, m."Code" as mat_code, m."Name" as mat_name, A.mat_pk
            , u."Name" as unit_name
	        , sum(A.suju_sum) over(partition by A.mat_pk) as tot_suju_sum
	        , sum(A.price_sum) over(partition by A.mat_pk) as tot_price_sum
	        , A.company_name, A.suju_sum, A.price_sum
	        from A 
	        inner join material m on m.id = A.mat_pk
            left join mat_grp mg on mg.id = m."MaterialGroup_id"
            left join unit u on u.id = m."Unit_id"
				""";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		
		return items;
	}

}
