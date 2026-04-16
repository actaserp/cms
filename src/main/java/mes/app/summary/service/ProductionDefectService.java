package mes.app.summary.service;

import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class ProductionDefectService {
	
	@Autowired
	SqlRunner sqlRunner;


	public List<Map<String, Object>> getList(String date_from, String date_to, Integer cboWorkCenter) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("date_from", date_from);
		paramMap.addValue("date_to", date_to);
		paramMap.addValue("cboWorkCenter", cboWorkCenter);

		String sql = """
			with ir_line as (
				select
					ir."InspectionDate",
					ir."WorkCenter_id",
					max(coalesce(ir."InspectionQty", 0)) as line_prod_qty
				from inspection_reports ir
				where ir."InspectionDate" between cast(:date_from as date) and cast(:date_to as date)
				  %s
				group by ir."InspectionDate", ir."WorkCenter_id"
			),
			ir_total as (
				select
					il."InspectionDate",
					sum(il.line_prod_qty) as total_prod_qty
				from ir_line il
				group by il."InspectionDate"
			)
			select
				dt.id as defect_pk,
				dt."Name" as defect_type,
				ir."InspectionDate" as "ProductionDate",
				sum(coalesce(ir."DefectQty", 0))::decimal as defect_qty,
				ir."WorkCenter_id",
				il.line_prod_qty,
				it.total_prod_qty
			from inspection_reports ir
				inner join defect_type dt on dt.id = ir."DefectType_id"
				left join ir_line  il
					on il."InspectionDate" = ir."InspectionDate"
				   and il."WorkCenter_id"  = ir."WorkCenter_id"
				left join ir_total it
					on it."InspectionDate" = ir."InspectionDate"
			where ir."InspectionDate" between cast(:date_from as date) and cast(:date_to as date)
			  %s
			group by dt.id, dt."Name", ir."InspectionDate", ir."WorkCenter_id",
					 il.line_prod_qty, it.total_prod_qty
		""";

		String wcFilter = (cboWorkCenter != null) ? "and ir.\"WorkCenter_id\" = :cboWorkCenter" : "";
		sql = String.format(sql, wcFilter, wcFilter);

		return this.sqlRunner.getRows(sql, paramMap);
	}

}
