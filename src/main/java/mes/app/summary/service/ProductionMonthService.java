package mes.app.summary.service;

import java.util.List;
import java.util.Map;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class ProductionMonthService {

	
	@Autowired
	SqlRunner sqlRunner;

	public List<Map<String, Object>> getList(String cboYear, String cbomatType, Integer matGrpPk, String cboDataDiv) {
		String tenantId = TenantContext.get();
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("cboYear", cboYear);
		paramMap.addValue("cbomatType", cbomatType);
		paramMap.addValue("matGrpPk", matGrpPk);
		paramMap.addValue("cboDataDiv", cboDataDiv);
		paramMap.addValue("spjangcd", tenantId);

		String data_column = "";
		
		String data_year = cboYear;
		
		paramMap.addValue("date_form",data_year+"-01-01" );
		paramMap.addValue("date_to",data_year+"-12-31" );
		
		
		if(cboDataDiv.equals("qty")) {
			data_column = "jr.\"GoodQty\" ";
		}else {
			data_column = "jr.\"GoodQty\" * m.\"UnitPrice\" ";
		}
		
		
		String sql ="""
				select jr."Material_id" as mat_pk
	            , fn_code_name('mat_type', mg."MaterialType") as mat_type_name, mg."Name" as mat_grp_name, m."Code" as mat_code, m."Name" as mat_name
                , u."Name" as unit_name
	            , sum(jr."GoodQty") as year_qty_sum
                , sum(jr."GoodQty" * m."UnitPrice") as year_money_sum
				""";
		
		for(int i=1; i<13; i++) {
			sql += " ,sum(case when extract(month from jr.\"ProductionDate\") = " + i + " then " + data_column + " end)as mon_" + i +"  ";
		}
		
		sql += """
				from job_res jr
	        inner join material m on m.id = jr."Material_id"
	        left join mat_grp mg on mg.id = m."MaterialGroup_id"
            left join unit u on u.id = m."Unit_id"
	        where jr."ProductionDate" between cast(:date_form as date) and cast(:date_to as date)
            and jr."State" = 'finished'
            and jr.spjangcd = :spjangcd
				""";
		
		if(cbomatType != null && !cbomatType.trim().isEmpty()) {
			sql +="""
					and mg."MaterialType" = :cbomatType
					""";
		}
		
		if(matGrpPk != null) {
			sql +="""
					and mg.id = :matGrpPk
					""";
		}
		
		sql += """
				group by jr."Material_id", mg."MaterialType", mg."Name", m."Name", m."Code", u."Name" 
				order by mg."MaterialType", mg."Name" , m."Name" , m."Code" 
				""";
		
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}
}
