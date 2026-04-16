package mes.app.production.service;

import java.util.List;
import java.util.Map;
import java.sql.Timestamp;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class ProdResultListService {

	@Autowired
	SqlRunner sqlRunner;	
	
	// 작업목록
	public List<Map<String, Object>> getProdResultList(String date_from, String date_to, String shift_code, Integer workcenter_pk, String spjangcd){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource(); 
		dicParam.addValue("date_from", Timestamp.valueOf(date_from + " 00:00:00"));
		dicParam.addValue("date_to", Timestamp.valueOf(date_to + " 23:59:59"));
		dicParam.addValue("shift_code", shift_code);
		dicParam.addValue("workcenter_pk", workcenter_pk);
		dicParam.addValue("spjangcd", spjangcd);
		
        String sql = """
	            select jr.id as pk
	                 , mp.id as mp_pk
		             , jr."WorkOrderNumber" as order_num
		             , to_char(mp."ProductionDate", 'yyyy-mm-dd') as prod_date
		             , wc."Name" as workcenter
		             , p."Name" as process
		             , mp."ShiftCode" as shift_code, sh."Name" as shift_name
		             , jr."WorkIndex" as work_idx    
		             , fn_code_name('job_state', jr."State") as job_state
		             , jr."State" as state
		             , m.id as mat_pk
		             , m."Code" as mat_code
		             , m."Name" as mat_name
		             , u."Name" as unit
		             , e."Name" as equipment
		             , jr."Description" as description 
		             , jr."OrderQty" as order_qty
		             , coalesce(mp."GoodQty", 0) as good_qty
		             , coalesce(mp."DefectQty", 0) as defect_qty
		             , coalesce(mp."LossQty", 0) as loss_qty
		             , coalesce(mp."ScrapQty", 0) as scrap_qty
	            from job_res jr 
	            inner join mat_produce mp on mp."JobResponse_id" = jr.id
	            left join material m on m.id = jr."Material_id"
	            left join unit u on u.id = m."Unit_id"
	            left join work_center wc on wc.id = mp."WorkCenter_id"
	            left join process p on p.id = wc."Process_id" 
	            left join equ e on e.id = mp."Equipment_id"
	            left join shift sh on sh."Code" = mp."ShiftCode"
	            where jr."ProductionDate" between :date_from and :date_to
	            and jr."State" = 'finished'
	            and jr.spjangcd = :spjangcd
            """;
        if (StringUtils.isEmpty(shift_code)==false) 
        	sql += " and mp.\"ShiftCode\" = :shift_code ";
        
        if (workcenter_pk != null) 
        	sql += " and mp.\"WorkCenter_id\" = :workcenter_pk ";

        sql += " order by jr.\"ProductionDate\" desc, jr.\"WorkOrderNumber\" asc ";
        		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}

	// 작업목록
	public List<Map<String, Object>> getProdProcessList(String date_from, String date_to, Integer process_pk, String spjangcd){

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("date_from", Timestamp.valueOf(date_from + " 00:00:00"));
		dicParam.addValue("date_to", Timestamp.valueOf(date_to + " 23:59:59"));
		dicParam.addValue("process_pk", process_pk);
		dicParam.addValue("spjangcd", spjangcd);

		String sql = """
	            select jr.id as pk
	                 , mp.id as mp_pk
		             , jr."WorkOrderNumber" as order_num
		             , to_char(mp."ProductionDate", 'yyyy-mm-dd') as prod_date
		             , wc."Name" as workcenter
		             , p."Name" as process
		             , mp."ShiftCode" as shift_code, sh."Name" as shift_name
		             , jr."WorkIndex" as work_idx    
		             , fn_code_name('job_state', jr."State") as job_state
		             , jr."State" as state
		             , m.id as mat_pk
		             , m."Code" as mat_code
		             , m."Name" as mat_name
		             , u."Name" as unit
		             , e."Name" as equipment
		             , jr."Description" as description 
		             , jr."OrderQty" as order_qty
		             , coalesce(mp."GoodQty", 0) as good_qty
		             , coalesce(mp."DefectQty", 0) as defect_qty
		             , coalesce(mp."LossQty", 0) as loss_qty
		             , coalesce(mp."ScrapQty", 0) as scrap_qty
		             , ROUND((
						   CASE
							   WHEN (COALESCE(mp."GoodQty", 0) + COALESCE(mp."DefectQty", 0)) = 0 THEN 0
							   ELSE (
								   COALESCE(mp."DefectQty", 0)::float /
								   (COALESCE(mp."GoodQty", 0) + COALESCE(mp."DefectQty", 0)) * 100
							   )
						   END
					   )::numeric, 2) AS defect_percent
	            from job_res jr 
	            inner join mat_produce mp on mp."JobResponse_id" = jr.id
	            left join material m on m.id = jr."Material_id"
	            left join unit u on u.id = m."Unit_id"
	            left join work_center wc on wc.id = mp."WorkCenter_id"
	            left join process p on p.id = wc."Process_id" 
	            left join equ e on e.id = mp."Equipment_id"
	            left join shift sh on sh."Code" = mp."ShiftCode"
	            where jr."ProductionDate" between :date_from and :date_to
	            and jr."State" = 'finished'
	            and jr.spjangcd = :spjangcd
            """;

		if (process_pk != null)
			sql += " and wc.\"Process_id\" = :process_pk ";

		sql += " order by jr.\"ProductionDate\" desc, jr.\"WorkOrderNumber\" asc ";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

		return items;
	}
	
	// 상세내역
	public Map<String, Object> getProdResultDetail(int mp_pk) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("mp_pk", mp_pk);
		
		String sql = """
			select jr.id
                 , jr."WorkOrderNumber" as order_num
                 , mp."LotNumber" as lot_num
                 , jr."State" as state
                 , fn_code_name('job_state', jr."State") as job_state
                 , jr."WorkIndex" as work_idx
                 , m.id as mat_pk, m."Code" as mat_code, m."Name" as mat_name
                 , m."LotSize"  as lot_size
                 , u."Name" as unit
                 , jr."OrderQty" as order_qty
                 , mp."GoodQty" as good_qty
                 , mp."DefectQty" as defect_qty
                 , mp."LossQty" as loss_qty
                 , mp."ScrapQty" as scrap_qty
                 , to_char(jr."ProductionDate", 'yyyy-mm-dd') as prod_date
                 , to_char(jr."StartTime", 'hh24:mi') as start_time
                 , to_char(jr."EndTime", 'yyyy-mm-dd') as end_date
                 , to_char(jr."EndTime", 'hh24:mi') as end_time
                 , jr."ShiftCode" as shift_code, sh."Name" as shift_name
                 , wc.id as workcenter_id, wc."Name" as workcenter_name
                 , p.id as process_id, p."Name" as process_name
                 , e.id as equipment_id, e."Name" as equipment_name
                 , jr."Description" as description 
	          from mat_produce mp 
              inner join job_res jr on jr.id = mp."JobResponse_id"
	          left join material m on m.id = jr."Material_id"
	          left join unit u on u.id = m."Unit_id"
	          left join work_center wc on wc.id = jr."WorkCenter_id"
              left join process p on p.id = wc."Process_id" 
	          left join equ e on e.id = mp."Equipment_id"
              left join shift sh on sh."Code" = jr."ShiftCode"
             where mp.id = :mp_pk
			""";
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

		return item;
	}

	// 부적합내역 조회
	public List<Map<String, Object>> getProdResultDefectList(int mp_pk) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("mp_pk", mp_pk);
		
		String sql = """
	        select dt.id as defect_id
	             , dt."Name" as defect_type 
	             , coalesce(jrd."DefectQty", 0) as defect_qty
	             , jrd."Description" as defect_remark
              from job_res_defect jrd 
             inner join defect_type dt on dt.id = jrd."DefectType_id" 
    	     where jrd."JobResponse_id" = :mp_pk
			""";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

		return items;
	}

	// 투입목록조회
	public List<Map<String, Object>> getProdResultConsumedList(int mp_pk) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("mp_pk", mp_pk);
		
		String sql = """
                select m.id as mat_pk
                , m."Name" as mat_name
                , u."Name" as unit
                , coalesce(mc."ConsumedQty", 0) as consumed
                , coalesce(mc."BomQty", 0) as bom_consumed
                , coalesce(mc."ScrapQty", 0) as scrap_consumed
                , coalesce(mc."AddQty", 0) as add_consumed
                , to_char(mc."StartTime", 'hh24:mi') as consumed_start
                , to_char(mc."EndTime", 'hh24:mi') as consumed_end 
                FROM mat_produce mp
				JOIN mat_consu mc ON mc."JobResponse_id" = mp."JobResponse_id" AND mc."LotIndex" = mp."LotIndex"
					inner join material m on m.id = mc."Material_id"
					left join unit u on u.id = m."Unit_id"
				WHERE mp.id = :mp_pk
			""";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

		return items;
	}

}
