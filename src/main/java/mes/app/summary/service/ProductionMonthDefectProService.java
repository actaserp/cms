package mes.app.summary.service;

import java.util.List;
import java.util.Map;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class ProductionMonthDefectProService {

	@Autowired
	SqlRunner sqlRunner;
	
	
	public List<Map<String, Object>> getList(String cboYear, String cboMatType, Integer cboMatGrpPk,String cboDataDiv,String chkOnlyDefect) {
		String tenantId = TenantContext.get();
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("cboYear", cboYear);
		paramMap.addValue("cboMatType", cboMatType);
		paramMap.addValue("cboMatGrpPk", cboMatGrpPk);
		paramMap.addValue("cboDataDiv", cboDataDiv);
		paramMap.addValue("chkOnlyDefect", chkOnlyDefect);
		paramMap.addValue("spjangcd", tenantId);
		String data_column = "";
		
		String data_year = cboYear;
		
		paramMap.addValue("date_form",data_year+"-01-01" );
		paramMap.addValue("date_to",data_year+"-12-31" );
		
		if(cboDataDiv.equals("pro")) {
			data_column = "A.defect_pro";
		}else if(cboDataDiv.equals("qty")) {
			data_column = "A.defect_qty";
		}else {
			data_column = "A.defect_money";
		}
		
		
		String sql = """
				 with A as 
	            (
	            select jr."Material_id" as mat_pk
	            , fn_code_name('mat_type', mg."MaterialType") as mat_type_name, mg."Name" as mat_grp_name, m."Code" as mat_code, m."Name" as mat_name
                , u."Name" as unit_name
	            , extract (month from jr."ProductionDate") as data_month
	            , coalesce(sum(jr."DefectQty"),0) as defect_qty
                , sum(jr."DefectQty") * m."UnitPrice" as defect_money
	            ,(coalesce(sum(jr."GoodQty"),0) + coalesce(sum(jr."DefectQty"),0)) as prod_sum
	            , 100 * coalesce(sum(jr."DefectQty"),0) / nullif(coalesce(sum(jr."GoodQty"),0) + coalesce(sum(jr."DefectQty"),0),0 ) as defect_pro
	            from job_res jr
	            inner join material m on m.id = jr."Material_id"
	            left join mat_grp mg on mg.id = m."MaterialGroup_id"
                left join unit u on u.id = m."Unit_id"
	            where jr."ProductionDate" between cast(:date_form as date) and cast(:date_to as date)
                and jr."State" = 'finished'
                and jr.spjangcd = :spjangcd
				""";
		
		if(cboMatType != null && !cboMatType.trim().isEmpty()) {
			sql += """
					and mg."MaterialType" = :cboMatType
					""";
		}
		
		if(cboMatGrpPk != null) {
			sql += """
					and mg.id = :cboMatGrpPk
					""";
		}
		
		sql += """
				group by jr."Material_id", mg."MaterialType", mg."Name" , m."Name" , m."Code", m."UnitPrice"
                , u."Name"
                , extract (month from jr."ProductionDate") 
	            )
	            select A.mat_pk, A.mat_type_name, A.mat_grp_name, A.mat_code, A.mat_name
                , A.unit_name
                , round((100 * sum(defect_qty) / nullif(sum(prod_sum),0))::decimal,3) as year_defect_pro 
                , sum(defect_qty) as year_defect_qty 
                , sum(defect_money) as year_defect_money
                , sum(prod_sum) as prod_Sum
				""";
		
		for(int i=1; i<13; i++) {
			sql += ", round(min(case when A.data_month = "+i+" then "+data_column+" ::decimal end),3)::float as mon_"+i+"  ";
		}
		
		
		
		sql += """ 
				from A 
				group by A.mat_pk, A.mat_type_name, A.mat_grp_name, A.mat_code, A.mat_name, A.unit_name
				""";
		
		if(chkOnlyDefect.equals("checkd")) {
			sql += """
					having sum(defect_qty) > 0
					""";
		}
		
		sql += """
				order by A.mat_type_name, A.mat_grp_name, A.mat_name	
				""";
		
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		return items;
	}

}
