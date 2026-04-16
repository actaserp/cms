package mes.app.production.service;

import java.sql.Date;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class ProdPrepareService {

	@Autowired
	SqlRunner sqlRunner;
	
	// 작업지시내역 조회
	public List<Map<String, Object>> jobOrderSearch(String data_date, String shift_code, Integer workcenter_pk, String spjangcd) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("data_date", Date.valueOf(data_date));  
		paramMap.addValue("shift_code", shift_code);  
		paramMap.addValue("workcenter_pk", workcenter_pk);
		paramMap.addValue("spjangcd", spjangcd);
		
        String sql = """
        		select jr.id
                , jr."WorkOrderNumber" as work_order_number
                , to_char(jr."ProductionDate", 'yyyy-mm-dd') as production_date
                , jr."ShiftCode" as shift_code, sh."Name" as shift_name
                , wc."Name" as workcenter_name
                , jr."WorkIndex" as work_index
                , fn_code_name('mat_type', mg."MaterialType") as mat_type_name
                , mg."Name" as mat_grp_name
                , m."Code" as mat_code, m."Name" as mat_name, u."Name" as unit_name
                , jr."OrderQty" as order_qty
                , e."Name" as equip_name
                , jr."State" as state, fn_code_name('job_state', jr."State") as state_name
                , jr."Description" as description
                , jr."MaterialProcessInputRequest_id" as proc_input_req_id
                , jr."State"
                , fn_code_name('job_state', jr."State") as state_name
                from job_res jr 
                left join material m on m.id = jr."Material_id"
                left join mat_grp mg on mg.id = m."MaterialGroup_id"
                left join unit u on u.id = m."Unit_id"
                left join work_center wc on wc.id = jr."WorkCenter_id"
                left join equ e on e.id = jr."Equipment_id"
                left join shift sh on sh."Code" = jr."ShiftCode"
                where jr."ProductionDate" = :data_date and jr."State" = 'ordered'
                and jr.spjangcd = :spjangcd
        		""";
  		
	    if (StringUtils.isEmpty(shift_code) == false) {
	    	sql +=" and jr.\"ShiftCode\" = :shift_code ";
	    }
	    
        if (workcenter_pk != null) {
        	sql += " and jr.\"WorkCenter_id\" = :workcenter_pk ";
        }
          

        sql += " order by jr.\"ProductionDate\", jr.\"WorkIndex\", jr.\"ShiftCode\", jr.id ";
       
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
	}

	// 작업지시내역 조회
	public List<Map<String, Object>> bomDetailList(String jr_pks, String data_date) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("jr_pks", jr_pks);
		paramMap.addValue("data_date", Date.valueOf(data_date));  
		
        String sql = """
        		with A as (
	                select unnest(string_to_array(:jr_pks, ','))::int as jr_pk
	            ), jr as (
	                select jr."Material_id" as prod_pk, sum(jr."OrderQty") as order_sum
	                from A
	                inner join job_res jr on jr.id = A.jr_pk 
	                where jr."MaterialProcessInputRequest_id" is null 
	                and jr."State" in ( 'ordered', 'working')
	                group by jr."Material_id"
	            ), P as (
	                select string_agg(JR.prod_pk::text, ',') as prod_pks
	                from jr
	            ), B1 as 
	            (
	                select B.prod_pk, B.mat_pk, sum(bom_ratio) as bom_qty 
	                from P
	                inner join tbl_bom_detail (P.prod_pks, :data_date) B on 1 = 1
	                where B.b_level = 1
	                group by B.prod_pk, B.mat_pk
	            ), R as 
	            (
	                select B1.mat_pk
	                , fn_code_name('mat_type', mg."MaterialType") as mat_type_name
	                , mg."Name" as mat_group_name
	                , m."Code" as mat_code
	                , m."Name" as mat_name 
                    , u."Name" as unit_name
	                , sum(jr.order_sum * B1.bom_qty) as requ_qty
	                , m."CurrentStock" as cur_stock
	                , m."AvailableStock" as available_stock 
                    ,coalesce( m."ProcessSafetyStock",0) as proc_safety_stock
	                from B1 
	                inner join jr on jr.prod_pk = B1.prod_pk
	                inner join material m on m.id = B1.mat_pk
	                inner join mat_grp mg on mg.id = m."MaterialGroup_id"		
                    left join unit u on u.id = m."Unit_id"
	                group by B1.mat_pk, mg."MaterialType", mg."Name", m."Code", m."Name", u."Name"
	                , m."CurrentStock", m."AvailableStock", m."ProcessSafetyStock"
	            ), S as (
	                select R.mat_pk
	                ,coalesce( sum(case when sh."HouseType" = 'material' then mh."CurrentStock" end),0) as material_stock
	                ,coalesce( sum(case when sh."HouseType" = 'process' then mh."CurrentStock" end),0) as process_stock
	                from R 
	                inner join mat_in_house mh on mh."Material_id" = R.mat_pk 
	                inner join store_house sh on sh.id = mh."StoreHouse_id"
	                where sh."HouseType" in ('material', 'process')
	                group by R.mat_pk
	            )
	            select R.mat_pk, R.mat_type_name, R.mat_group_name, R.mat_code, R.mat_name
                , R.unit_name, R.requ_qty
	            , S.material_stock, S.process_stock
                , R.proc_safety_stock
                , R.cur_stock
	            , greatest(0, R.requ_qty + ( coalesce(R.proc_safety_stock,0) - greatest(0, S.process_stock) )) as input_req_qty 
	            from R 
	            left join S on S.mat_pk = R.mat_pk
                order by R.mat_type_name, R.mat_group_name, R.mat_code, R.mat_name
        		""";
  		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
	}
		
	// 투입요청내역 조회 
	public List<Map<String, Object>> matProcInputList (Integer req_pk) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("req_pk", req_pk);
		
        String sql = """
        		with R as (
                    select  mi."MaterialProcessInputRequest_id" as req_pk
                    , mi."Material_id" as mat_pk
                    , fn_code_name('mat_type', mg."MaterialType") as mat_type_name
                    , mg."Name" as mat_group_name
                    , m."Code" as mat_code
                    , m."Name" as mat_name 	
                    , u."Name" as unit_name
                    , mi."RequestQty" as req_qty
                    , m."CurrentStock" as cur_stock
                    ,coalesce( m."ProcessSafetyStock",0) as proc_safety_stock
                    , mi."MaterialStoreHouse_id", mi."ProcessStoreHouse_id"
                    , fn_code_name('mat_proc_input_state', mi."State") as state_name
                    from mat_proc_input mi
                    inner join material m on m.id = mi."Material_id"
                    inner join mat_grp mg on mg.id = m."MaterialGroup_id"	
                    left join unit u on u.id = m."Unit_id"
                    where mi."MaterialProcessInputRequest_id" = :req_pk
                ), S as (
                    select R.mat_pk
                    ,coalesce( sum(case when sh."HouseType" = 'material' then mh."CurrentStock" end),0) as material_stock
                    ,coalesce( sum(case when sh."HouseType" = 'process' then mh."CurrentStock" end),0) as process_stock
                    from R 
                    inner join mat_in_house mh on mh."Material_id" = R.mat_pk 
                    inner join store_house sh on sh.id = mh."StoreHouse_id"
                    where sh."HouseType" in ('material', 'process')
                    group by R.mat_pk
                )
                select R.mat_pk, R.mat_type_name, R.mat_group_name, R.mat_code, R.mat_name
                , R.req_pk
                , R.req_qty
                , R.state_name
                , R.unit_name
                , S.material_stock
                , S.process_stock 
                , R.proc_safety_stock
                from R 
                left join S on S.mat_pk = R.mat_pk
                order by R.mat_type_name, R.mat_group_name, R.mat_code, R.mat_name
        		""";
  		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
	}
}
