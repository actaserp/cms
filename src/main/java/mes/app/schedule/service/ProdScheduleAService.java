package mes.app.schedule.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class ProdScheduleAService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getProdScheduleList(String year, String month) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		
        String ym = year + '-' + String.format("%02d", Integer.parseInt(month));
        String ymd = year + '-' + String.format("%02d", Integer.parseInt(month)) + "-01";
        paramMap.addValue("ymd", ymd);
        paramMap.addValue("ym", ym);
        
        String sql = """
        		select  id, "DataYear" as year, "WeekIndex", "StartDate", "EndDate"
                , concat(to_char("StartDate", 'mm.dd'), '~', to_char("EndDate", 'mm.dd')) as period_date 
                , "PlanDate", "State"
                , fn_code_name('prod_week_term_state', "State") as "StateName"
                from prod_week_term pwt
                where "EndDate" >= cast(:ymd as date)
                and "StartDate" < cast(:ymd as date) + interval ' 1 months'
                order by "StartDate"
        		""";
        
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public List<Map<String, Object>> getSujuList(String pwtId) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("pwtId", pwtId);
        
        String sql = """
        		with W as (
                select id
                ,"DataYear","WeekIndex", "StartDate" , "EndDate"
                , concat(to_char("StartDate", 'mm.dd'), '~', to_char("EndDate", 'mm.dd')) as period_date 
                , "PlanDate" , "State"
                , fn_code_name('prod_week_term_state', "State") as StateName
                from prod_week_term 
                where id = cast(:pwtId as Integer)
                )
                select s.id as suju_id
                , W.id as pwt_id
                , W.period_date
                ,s."Company_id"
                ,c."Name"  as "CompanyName" 
                ,s."Material_id",mg."Name" as mat_group_name,m2."Code" as mat_code,m2."Name" as mat_name
                , u."Name" as "UnitName"
                ,s."SujuQty" as "SujuQty", s."SujuQty2"
                , case when m2."PackingUnitQty" > 0 then round((s."SujuQty" / m2."PackingUnitQty")::numeric, 2)
                    else null end as box_qty
                ,s."JumunDate", s."ProductionPlanDate", s."DueDate"
                ,s."State"
                ,fn_code_name('suju_state', s."State") as "StateName"
                ,s."Description" 
                ,s."PlanDataPk"
                from  suju s
                left join company c on c.id = s."Company_id" 
                left join material m2 on m2.id = s."Material_id" 
                left join mat_grp mg on mg.id = m2."MaterialGroup_id" 
                left join unit u on u.id = m2."Unit_id"
                inner join W on (s."PlanDataPk" = W.id and s."PlanTableName"='prod_week_term') 
                            or (s."ProductionPlanDate" between W."StartDate" and W."EndDate" 
                                and s."State" = 'received' )
        		""";
        
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public List<Map<String, Object>> getMaterialRequirementList(String srcTable, String pwtId, String matType) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		
		paramMap.addValue("srcTable", srcTable);
		paramMap.addValue("srcDataPk", pwtId);
        paramMap.addValue("matType", matType);

        String sql = """
    		select mr.id 
            , mg."Name" as mat_group_name
            , mr."Material_id"  
            , m."Code"  as mat_code
            , m."Name" as mat_name
            , u."Name" as "UnitName"
            , coalesce(m."SafetyStock", 0) as "SafetyStock" /* 안전재고 */
            , coalesce(mr."RequireQty1",0) as "RequireQty1" /*필요량1*/
            , coalesce(mr."RequireQty2", 0) as "RequireQty2" /*필요량2=필요량1-예약량*/
            , coalesce(mr."AvailableStock",0) as "AvailableStock" /*가용재고*/
            , coalesce(mr."ReservationStock",0) as "ReservationStock" /* 예약량 */
            , greatest(0, least(coalesce(mr."AvailableStock",0), coalesce(mr."RequireQty1",0))) as "ReservationStock_input"
            , case when mr."ReservationStock" > 0 then greatest(0, (mr."RequireQty1" + (coalesce(m."SafetyStock", 0) - ( coalesce(mr."AvailableStock",0)-coalesce(mr."ReservationStock",0) ) )-coalesce(mr."ReservationStock",0) ) -coalesce(mr."RequestQty",0) )
        	    else greatest(0, mr."RequireQty1" +  greatest(0, coalesce(m."SafetyStock", 0) - coalesce(mr."AvailableStock",0) ) - coalesce(mr."RequestQty",0)) end as "RequestQty_input"
            , greatest(greatest((mr."RequireQty1" + (coalesce(m."SafetyStock", 0) - (coalesce(mr."AvailableStock",0)-coalesce(mr."ReservationStock",0)))-coalesce(mr."ReservationStock",0) ), 0)-coalesce(mr."RequestQty",0),0)  as "RequestQty_input2"
            , coalesce(mr."RequestQty",0) as "RequestQty" /* 생산요청량 */
            , to_char(mr."RequestDate",'yyyy-mm-dd hh24:mi') as "RequestDate"
            , to_char(mr._modified,'yyyy-mm-dd hh24:mi:ss') as modified
            from mat_requ mr
                left join material m on m.id=mr."Material_id" 
                left join mat_grp mg on mg.id = m."MaterialGroup_id" 
                left join unit u  on u.id =m."Unit_id" 
            where  mr."MaterialType" = :matType
            and mr."SourceDataPk" = cast(:srcDataPk as Integer) 
            and mr."SourceTableName"= :srcTable
        	""";
        
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public boolean saveProdRequCalc(String srcTable, String srcDataPk, String matType, int userId) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		
		paramMap.addValue("srcTable", srcTable);
		paramMap.addValue("srcDataPk", srcDataPk);
        paramMap.addValue("matType", matType);
        paramMap.addValue("userId", userId);
        
        String sql = """
        		with A as (
                select s."Material_id" as product_id
			    , sum(s."SujuQty") as prod_qty
                from suju s
                where s."PlanTableName" = :srcTable 
                and s."PlanDataPk" = cast(:srcDataPk as Integer)
                group by s."Material_id"
                )
	            insert into mat_requ("MaterialType", "Material_id", "SourceTableName", "SourceDataPk"
	            , "RequireQty1", "RequireQty2"
	                ,"AvailableStock", "SafetyStock", "_created", "_modified", "_creater_id")
	            select :matType 
	            , A.product_id
	            , :srcTable as "SourceTableName"
	            , cast(:srcDataPk as Integer) as "SourceDataPk"
	            , A.prod_qty as req_qty1, A.prod_qty as req_qty2
	            , coalesce(m."AvailableStock", 0)
	            , m."SafetyStock"
	            , now() as "_created"
	            , now() as "_modified"
	            , :userId as "_creater_id"
	            from A
	            inner join material m on m.id = A.product_id
	            --group by A.product_id, m."AvailableStock",  m."SafetyStock"
        		""";
        
        boolean flag = false;
        
        int count = this.sqlRunner.execute(sql, paramMap);
        
        if (count > 0) {
        	flag = true;
        }
		return flag;
	}

	public boolean saveMatRequCalc(String srcTable, String srcDataPk, String matType, int userId) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		
		paramMap.addValue("srcTable", srcTable);
		paramMap.addValue("srcDataPk", srcDataPk);
        paramMap.addValue("matType", matType);
        paramMap.addValue("userId", userId);
        
        String sql = """
    			with mreq as (
                select mr."Material_id" as product_id
                , mr."RequestQty" as prod_qty
                from mat_requ mr
                where mr."SourceTableName" = :srcTable
                and mr."SourceDataPk" = cast(:srcDataPk as Integer)
        		""";
        
        if(matType.equals("semi")) {
        	sql += " and mr.\"MaterialType\" = 'product' ";
        } else if (matType.equals("material")) {
        	sql += " and mr.\"MaterialType\" in ( 'product', 'semi') ";	
        }
        
        sql += """
        		), agg as 
	            (
	                select string_agg(mreq.product_id::text,',') as prod_ids 
	                from mreq
	            ), B as (
                select bm.prod_pk
                , bm.mat_pk
                , sum(bm.bom_ratio) as req_qty
                from agg 
                inner join tbl_bom_detail(agg.prod_ids,to_char(now(),'yyyy-mm-dd') ) bm on 1 = 1
                inner join material m on m.id = bm.mat_pk
                inner join mat_grp mg on mg.id = m."MaterialGroup_id"
                where 1 = 1
        		""";
        
        if(matType.equals("semi")) {
        	sql += " and mg.\"MaterialType\" = 'semi' ";
        } else if (matType.equals("material")) {
        	sql += " and mg.\"MaterialType\" in ('raw_mat','sub_mat') and bm.b_level = 1 ";	
        }
        
        sql += """
        		group by bm.prod_pk, bm.mat_pk
	            )
	            insert into mat_requ("MaterialType", "Material_id", "SourceTableName", "SourceDataPk", "RequireQty1"
	                ,"AvailableStock", "SafetyStock", "_created", "_modified", "_creater_id")
	            select :matType as "MaterialType" 
	            , B.mat_pk as "Material_id"
	            , :srcTable as "SourceTableName"
	            , cast(:srcDataPk as Integer) as "SourceDataPk"
	            --, sum(mreq.prod_qty * b.req_qty) as "RequireQty1"
	            , fn_unit_ceiling( sum(mreq.prod_qty * b.req_qty), u."PieceYN") as "RequireQty1"
	            , coalesce(m."AvailableStock", 0)
	            , m."SafetyStock"
	            , now() as "_created"
	            , now() as "_modified"
	            , cast(:userId as Integer) as "_creater_id"
	            from B
	            inner join mreq on mreq.product_id=B.prod_pk
	            inner join material m on m.id = b.mat_pk
	            left join unit u on u.id = m."Unit_id"
	            group by B.mat_pk, m."AvailableStock",  m."SafetyStock", u."PieceYN"
        		""";
        boolean flag = false;
        
        int count = this.sqlRunner.execute(sql, paramMap);
        
        if (count > 0) {
        	flag = true;
        }
		return flag;
	}

}
