package mes.app.production.service;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class ProdOrderEditService {

	@Autowired
	SqlRunner sqlRunner;
	
	// 수주 목록 조회
	public List<Map<String, Object>> getSujuList(String date_kind, String start, String end, Integer mat_group, String mat_name, String not_flag, String spjangcd) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("start", Timestamp.valueOf(start + " 00:00:00"));
		dicParam.addValue("end", Timestamp.valueOf(end + " 23:59:59"));
		dicParam.addValue("mat_group", mat_group);
		dicParam.addValue("mat_name", mat_name);
		dicParam.addValue("spjangcd", spjangcd);
		
		if (StringUtils.isEmpty(date_kind)) {
			date_kind = "sales";
		}
		
		// 수주에서 수주량-예약량 = 수주량2(필요량)
        String sql = """
        		with s as (
	                select s.id, s."JumunDate", s."DueDate", s."JumunNumber"
	                , s."CompanyName"
	                , s."Material_id"
	                , mg."Name" as "MaterialGroupName"
	                , mg.id as "MaterialGroup_id"
	                , m."Code" as mat_code
	                , m."WorkCenter_id" as workcenter_id
	                , m."Name" as mat_name
	                , u."Name" as unit_name
	                , s."SujuQty"
	                , s."SujuQty2"
	                , coalesce (s."ReservationStock",0) as "ReservationStock"
	                , fn_code_name('suju_state', s."State") as "StateName"
	                , fn_code_name('mat_type', mg."MaterialType") as mat_type_name
	                , s."State"
	                , s."Description" as description
	                from suju s
	                inner join material m on m.id = s."Material_id"
	                inner join mat_grp mg on mg.id = m."MaterialGroup_id"
	                left join unit u on m."Unit_id" = u.id
	                where 1 = 1 and mg."MaterialType"!='sangpum'
	                and s.spjangcd = :spjangcd
	                and s.confirm = '1'
        		""";
        
        if ("suju_date".equals(date_kind)) {
        	sql += " and s.\"JumunDate\" between :start and :end ";
        } else {
            sql += " and s.\"DueDate\" between :start and :end ";
        }
        
        if (mat_group != null) {
        	sql += " and mg.id = :mat_group ";
        }
        
        if (StringUtils.isEmpty(mat_name) == false) {
        	sql += """
        			and ( upper(m."Name") like concat('%%',upper(:mat_name),'%%')
	                or upper(m."Code") = upper(:mat_name)
	                )
        			""";
        }
        
        sql += """
        		)
	            , q as (
	                select s.id as suju_id
	                , sum(jr."OrderQty") as ordered_qty
	                from job_res jr 
	                inner join s on s.id = jr."SourceDataPk" 
	                and jr."SourceTableName"='suju' 
	                and jr."Material_id" = s."Material_id"
	                where jr."State" <>'canceled'
	                group by s.id
	            )
	            select s.id
	            , s."JumunNumber"
	            , to_char(s."JumunDate", 'yyyy-mm-dd') as "JumunDate"
	            , to_char(s."DueDate", 'yyyy-mm-dd') as "DueDate"
	            , s."CompanyName"
	            , s.mat_type_name
	            , s."MaterialGroupName"
	            , s.mat_code
	            , s.workcenter_id
	            , s.mat_name
	            , s.unit_name
	            , s."Material_id" as mat_pk
	            , s."SujuQty" as "SujuQty"
	            , s."SujuQty2" as "SujuQty2"
	            , s."ReservationStock" as "ReservationStock"
	            , coalesce(q.ordered_qty,0) as ordered_qty
	            , greatest(0, s."SujuQty2"- coalesce (q.ordered_qty,0)) as remain_qty
	            , 0 as "AdditionalQty"
	            , s.description
	            , s."StateName", s."State"
	            from s 
	            left join q on q.suju_id = s.id
	            where 1 = 1
        		""";

        if (StringUtils.isEmpty(not_flag) == false) {
        	sql += "  and (s.\"SujuQty2\"- coalesce (q.ordered_qty,0)) > 0 ";
        } 

        if ("suju_date".equals(date_kind)) {
        	sql += " order by s.\"DueDate\" desc, s.\"JumunNumber\" desc ";
        } else {
            sql += " order by s.\"JumunDate\" desc, s.\"JumunNumber\" desc ";
        }
        		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}

	public List<Map<String, Object>> makeProdOrder(Integer sujuId) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("sujuId", sujuId);
		
		String sql = """
                with qsum as(
                select
                s.id as suju_id
                , s."Material_id"
                , s."SujuQty"
                , (select sum(jr."OrderQty") from job_res jr where jr."SourceDataPk" = s.id and jr."SourceTableName"='suju' and jr."State" <>'canceled' ) as ordered_qty
                from suju s
                where s.id = :sujuId
                )
                select suju_id, "Material_id", "SujuQty", ordered_qty, greatest("SujuQty"-ordered_qty, 0 ) as remain_qty
                from qsum
				""";
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}
	
	// 제품 지시내역 조회
	public List<Map<String, Object>> getJobOrderList(Integer suju_id) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("suju_id", suju_id);
		
		String sql = """
				select jr.id
	            , jr."WorkOrderNumber"
	            , jr."ProductionDate"
	            , jr."ShiftCode" 
	            , s."Name" as "ShiftName"
	            , m."Code" as mat_code
	            , m."Name" as mat_name
	            , u."Name" as unit_name
	            , jr."OrderQty" as "OrderQty"
	            , jr."WorkCenter_id" 
	            , wc."Name" as "WorkcenterName"
	            , jr."Equipment_id"
	            , e."Name" as "EquipmentName"
	            , jr."State" 
	            , fn_code_name('job_state', jr."State") as "StateName"
	            from job_res jr 
	            inner join material m on m.id = jr."Material_id" 
	            inner join mat_grp mg on mg.id = m."MaterialGroup_id" 
	            left join unit u on u.id = m."Unit_id" 
	            left join shift s on s."Code" = jr."ShiftCode" 
	            left join work_center wc on wc.id = jr."WorkCenter_id"
	            left join equ e on e.id = jr."Equipment_id"
	            where jr."SourceDataPk"=:suju_id
	            and jr."SourceTableName" ='suju'
	            and mg."MaterialType" in ('product')
	            order by jr."WorkOrderNumber" desc, jr.id
				""";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

		return items;
	}

	// 제품 지시내역 상세조회
	public Map<String, Object> getJobOrderDetail(Integer jobres_id) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("jobres_id", jobres_id);
		
		String sql = """
				select jr.id
	            , jr."WorkOrderNumber"
	            , to_char(jr."ProductionDate", 'yyyy-mm-dd') as "ProductionDate"
	            , jr."Material_id"
	            , jr."ShiftCode" 
	            , s."Name" as "ShiftName"
	            , m."Name" as mat_name
	            , u."Name" as unit_name
	            , jr."OrderQty" as "OrderQty"
	            , jr."WorkCenter_id" 
	            , jr."Equipment_id"
	            , jr."State" 
	            , fn_code_name('job_state', jr."State") as "StateName"
	            , jr."Description"
	            from job_res jr 
	            inner join material m on m.id = jr."Material_id" 
	            left join unit u on u.id = m."Unit_id" 
	            left join shift s on s."Code" = jr."ShiftCode" 
	            left join work_center wc on wc.id = jr."WorkCenter_id"
	            where jr.id = :jobres_id
			""";
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

		return item;
	}
	
	// 반제품 작업지시 조회
	public List<Map<String, Object>> getSemiList(String data_date, Integer mat_pk, Integer suju_qty, Integer suju_pk) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("data_date", data_date);
		dicParam.addValue("mat_pk", mat_pk.toString());
		dicParam.addValue("mat_order_qty", suju_qty);
		dicParam.addValue("suju_pk", suju_pk);
		
		String sql = """
				with A as (
	                select m.id as mat_pk
	                , mg."Name" as group_name
	                , m."Name" as mat_name
	                , m."Code" as mat_code
	                , m."WorkCenter_id" as workcenter_id
	                , m."StoreHouse_id" as storehouse_id
	                , u."Name" as unit_name
	                , fn_unit_ceiling( bom.bom_ratio * :mat_order_qty, u."PieceYN" ) as bom_qty
	                , fn_unit_ceiling( bom.bom_ratio * :mat_order_qty, u."PieceYN" ) as order_qty
	                from tbl_bom_detail(:mat_pk, :data_date) as bom
	                inner join material m on m.id = bom.mat_pk
	                left join unit u on u.id = m."Unit_id"
	                inner join mat_grp mg on mg.id = m."MaterialGroup_id" 
	                where mg."MaterialType" in ('semi')
	                ), 
	                sq as (                
					select 
					 s.id as suju_pk
					 ,jr."Material_id" as mat_pk
					 , sum(jr."OrderQty") as ordered_qty
					from job_res jr 
					 inner join suju s on s.id=jr."SourceDataPk" and jr."SourceTableName" ='suju'
					 inner join material m on m.id=jr."Material_id" 
					 inner join mat_grp mg on mg.id=m."MaterialGroup_id"  
					where 
					s.id = :suju_pk
					and mg."MaterialType" ='semi'
					group by s.id, jr."Material_id" 
	                )
	                select A.*, sq.ordered_qty
	                from A
	                left join sq on sq.mat_pk = A.mat_pk
				""";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

		return items;
	}
	
	// 반제품 지시내역 조회
	public List<Map<String, Object>> getSemiJoborderList(Integer suju_id) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("suju_id", suju_id);
		
		String sql = """
				select jr.id
	            , jr."WorkOrderNumber"
	            , jr."ProductionDate"
	            , jr."ShiftCode" 
	            , s."Name" as "ShiftName"
	            , m."Code" as mat_code
	            , m."Name" as mat_name
	            , u."Name" as unit_name
	            , jr."OrderQty" as "OrderQty"
	            , jr."WorkCenter_id" 
	            , wc."Name" as "WorkcenterName"
	            , jr."Equipment_id"
	            , e."Name" as "EquipmentName"
	            , jr."State" 
	            , fn_code_name('job_state', jr."State") as "StateName"
	            from job_res jr 
	            inner join material m on m.id = jr."Material_id" 
	            inner join mat_grp mg on mg.id = m."MaterialGroup_id" 
	            left join unit u on u.id = m."Unit_id" 
	            left join shift s on s."Code" = jr."ShiftCode" 
	            left join work_center wc on wc.id = jr."WorkCenter_id"
	            left join equ e on e.id = jr."Equipment_id"
	            where jr."SourceDataPk"=:suju_id 
	            and jr."SourceTableName" ='suju'
	            and mg."MaterialType" in ('semi')
	            order by jr."WorkOrderNumber" desc, jr.id
				""";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

		return items;
	}
	
	
}
