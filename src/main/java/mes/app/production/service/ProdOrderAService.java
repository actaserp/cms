package mes.app.production.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class ProdOrderAService {
	
	@Autowired
	SqlRunner sqlRunner;

	public List<Map<String, Object>> getProdOrderA(String dateFrom, String dateTo, String matGrpPk, String keyword,
			String matType, String workcenterPk, String spjangcd) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("dateFrom", dateFrom);
		paramMap.addValue("dateTo", dateTo);
		paramMap.addValue("matGrpPk", matGrpPk);
		paramMap.addValue("matType", matType);
		paramMap.addValue("workcenterPk", workcenterPk);
		paramMap.addValue("keyword", keyword);
		paramMap.addValue("spjangcd", spjangcd);
		
		String sql = """
		        select jr.id
		        , jr."WorkOrderNumber" as workorder_number
                , to_char(jr."ProductionDate", 'yyyy-mm-dd') as production_date
	            , jr."ShiftCode" as shift_code, sh."Name" as shift_name
	            , fn_code_name('mat_type', mg."MaterialType") as mat_type_name
	            , mg."Name" as mat_grp_name
	            , m."Code" as mat_code
	            , m."Name" as mat_name
	            , u."Name" as unit_name
                , case when m."PackingUnitQty" > 0 then round((jr."OrderQty" / m."PackingUnitQty")::numeric, 0)
                        else null end as box_qty
	            , jr."OrderQty" as order_qty
	            , wc."Name" as workcenter_name, e."Name" as equip_name
	            , jr."State" as state, fn_code_name('job_state', jr."State") as state_name
                , jr."Description" as description
                , up."Name" as creator
	            from job_res jr 
	            left join material m on m.id = jr."Material_id"
	            left join mat_grp mg on mg.id = m."MaterialGroup_id"
	            left join unit u on u.id = m."Unit_id"
	            left join work_center wc on wc.id = jr."WorkCenter_id"
	            left join equ e on e.id = jr."Equipment_id"
	            left join shift sh on sh."Code" = jr."ShiftCode"
                left join user_profile up on up."User_id" = jr."_creater_id"
                where jr."ProductionDate" between cast(:dateFrom as date) and cast(:dateTo as date)
                and jr.spjangcd = :spjangcd
				""";
		if (StringUtils.isEmpty(workcenterPk) == false) sql += " and jr.\"WorkCenter_id\" = cast(:workcenterPk as Integer) ";
		if (StringUtils.isEmpty(matGrpPk) == false) {
			sql += " and mg.id = cast(:matGrpPk as Integer) ";
		} else if (StringUtils.isEmpty(matType) == false) {
			sql += " and mg.\"MaterialType\" = :matType ";
		}
		if (StringUtils.isEmpty(keyword) == false) sql += " and (m.\"Name\" like concat('%%', :keyword , '%%') or m.\"Code\" like concat('%%', :keyword , '%%') ) ";
        sql += " order by jr.\"WorkOrderNumber\" desc, jr.\"ProductionDate\" desc, jr.\"ShiftCode\", jr.id ";
        
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

		return items;
	}

	public Map<String, Object> getMatInfo(String id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("matPk", id);
		
		if (id.isEmpty()) {
			return null;
		}
		String sql = """
				select e.id as equip_pk, e."Name" as equipment_name
	            , wc.id as workcenter_pk, wc."Name" as workcenter_name
	            , u."Name" as unit_name
	            from material m 
	            left join unit u on u.id = m."Unit_id"
	            left join work_center wc on wc.id = m."WorkCenter_id"
	            left join equ e on e.id = m."Equipment_id"
	            where m.id = cast(:matPk as Integer)
				""";
		
		Map<String, Object> items = this.sqlRunner.getRow(sql, paramMap);

		return items;
	}

	public Map<String, Object> getProdOrderADetail(String jrPk) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("jrPk", jrPk);
		
		String sql = """
	            select 
	            jr.id
	            , jr."WorkOrderNumber" as workorder_number
                , to_char(jr."ProductionDate", 'yyyy-mm-dd') as production_date
	            , jr."ShiftCode" as shift_code
	            , sh."Name" as shift_name
                , mg."MaterialType" as mat_type
	            , fn_code_name('mat_type', mg."MaterialType") as mat_type_name
	            , mg.id as mat_grp_id
	            , mg."Name" as mat_grp_name
	            , m.id as mat_id
	            , m."Code" as mat_code
	            , m."Name" as mat_name
                , m."Unit_id" as unit_id
                ,  u."Name" as unit_name
	            , jr."OrderQty" as order_qty
                , case when m."PackingUnitQty" > 0 then round((jr."OrderQty" / m."PackingUnitQty")::numeric, 0)
                  else 
                  null 
                  end as box_order_qty
                , wc.id as workcenter_id
                , wc."Name" as workcenter_name
                , e.id as equip_id
                , e."Name" as equip_name
	            , jr."State" as state
	            , fn_code_name('job_state', jr."State") as state_name
                , jr."Description" as description
	            from job_res jr 
	            left join material m on m.id = jr."Material_id"
	            left join mat_grp mg on mg.id = m."MaterialGroup_id"
	            left join unit u on u.id = m."Unit_id"
	            left join work_center wc on wc.id = jr."WorkCenter_id"
	            left join equ e on e.id = jr."Equipment_id"
	            left join shift sh on sh."Code" = jr."ShiftCode"
                where jr.id = cast(:jrPk as Integer)
				""";
		
		Map<String, Object> items = this.sqlRunner.getRow(sql, paramMap);

		return items;
	}

	public Map<String, Object> getJopResRow(Integer id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);
		
		String sql = """
				select "State" as state
	            , "SourceDataPk" as src_pk, "SourceTableName" as src_table
	            from job_res 
	            where id = :id
				""";

		Map<String,Object> items = this.sqlRunner.getRow(sql, paramMap);
		
		return items;
	}

	public int deleteById(Integer id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);
		
		String sql = """
				delete from job_res
                where id = :id
                and "State" = 'ordered'
				""";
		
		int value = this.sqlRunner.execute(sql, paramMap);
		return value;
	}

	public void updateBySujuPk(Integer sujuPk) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("sujuPk", sujuPk);
		
		String sql = """
					update suju
	                set "State" = 'received'
	                where id = :sujuPk
	                and "State" = 'ordered'
				""";
		
		this.sqlRunner.execute(sql, paramMap);
	}

}
