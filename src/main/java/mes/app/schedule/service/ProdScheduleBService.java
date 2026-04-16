package mes.app.schedule.service;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.entity.MatRequ;
import mes.domain.repository.MatRequRepository;
import mes.domain.services.SqlRunner;

@Service
public class ProdScheduleBService {

	@Autowired
	SqlRunner sqlRunner;

	@Autowired
	MatRequRepository matRequRepository;
	
	// 소요량산출 헤드리스트 (생산계획 리스트)
	public List<Map<String, Object>> getBundleHeadList(Timestamp start, Timestamp end) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource(); 
		dicParam.addValue("start", start);
		dicParam.addValue("end", end);

        String sql = """
        		select
	            id as head_id
	            , to_char(_created,'yyyy-mm-dd hh24:mi') as created
	            , to_char(_modified,'yyyy-mm-dd hh24:mi') as modified
	            , fn_code_name('prod_week_term_state',_status) as state_name
	            from bundle_head bh 
	            where 
	              bh."TableName"='suju'
	              and _created between :start and :end
	              """;
        		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}
	
	// not bundled suju list (수주추가탭 리스트)
	public List<Map<String, Object>> getNotCountedSujuList(Timestamp start, Timestamp end) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource(); 
		dicParam.addValue("start", start);
		dicParam.addValue("end", end);

        String sql = """
        		select s.id
	            , s."JumunNumber"
	            , s."Material_id" as "Material_id"
	            , mg."Name" as "MaterialGroupName"
	            , mg.id as "MaterialGroup_id"
	            , m.id as "Material_id"
	            , m."Code" as product_code
	            , m."Name" as product_name
	            , u."Name" as unit_name
	            , s."SujuQty"
	            , to_char(s."JumunDate", 'yyyy-mm-dd') as "JumunDate"
	            , to_char(s."DueDate", 'yyyy-mm-dd') as "DueDate"
	            , s."CompanyName"
	            , c."Name" as company_name
	            , s."Company_id"
	            , to_char(s."ProductionPlanDate", 'yyyy-mm-dd') as production_plan_date
	            , to_char(s."ShipmentPlanDate", 'yyyy-mm-dd') as shiment_plan_date
	            , s."Description"
	            , s."AvailableStock"
	            , s."ReservationStock"
	            , s."SujuQty2"
	            , fn_code_name('suju_state', s."State") as "StateName"
	            , s."State"
	            , to_char(s."_created", 'yyyy-mm-dd') as create_date
	            , s."PlanDataPk" as head_id , s."PlanTableName"
	            from suju s
	            inner join material m on m.id = s."Material_id"
	            inner join mat_grp mg on mg.id = m."MaterialGroup_id"
	            left join unit u on m."Unit_id" = u.id
	            left join company c on c.id= s."Company_id"
	            where s."ProductionPlanDate" between :start and :end
	            and s."PlanDataPk" is null
	            and s."State" = 'received';
	              """;
        		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}
		
	// bundled suju list (수주내역탭 리스트)
	public List<Map<String, Object>> getBundledSujuList(Integer head_id) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource(); 
		dicParam.addValue("head_id", head_id);

        String sql = """
        		select s.id
	            , s."JumunNumber"
	            , s."Material_id" as "Material_id"
	            , mg."Name" as "MaterialGroupName"
	            , mg.id as "MaterialGroup_id"
	            , m.id as "Material_id"
	            , m."Code" as product_code
	            , m."Name" as product_name
	            , u."Name" as unit_name
	            , s."SujuQty"
	            , to_char(s."JumunDate", 'yyyy-mm-dd') as "JumunDate"
	            , to_char(s."DueDate", 'yyyy-mm-dd') as "DueDate"
	            , s."CompanyName"
	            , c."Name" as company_name
	            , s."Company_id"
	            , to_char(s."ProductionPlanDate", 'yyyy-mm-dd') as production_plan_date
	            , to_char(s."ShipmentPlanDate", 'yyyy-mm-dd') as shiment_plan_date
	            , s."Description"
	            , s."AvailableStock"
	            , s."ReservationStock"
	            , s."SujuQty2"
	            , fn_code_name('suju_state', s."State") as "StateName"
	            , s."State"
	            , to_char(s."_created", 'yyyy-mm-dd') as create_date
	            , s."PlanDataPk" as head_id , s."PlanTableName"
	            from suju s
	            inner join material m on m.id = s."Material_id"
	            inner join mat_grp mg on mg.id = m."MaterialGroup_id"
	            left join unit u on m."Unit_id" = u.id
	            left join company c on c.id= s."Company_id"
	            where s."PlanDataPk" = :head_id 
	            and s."PlanTableName"='bundle_head'
	              """;
        		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}
	
	// 제품필요량 조회
	public List<Map<String, Object>> getMaterialRequirementList(String src_table, Integer src_data_pk, String mat_type) {
		
		/*
			RequireQty1 : 수주량 합계
            RequireQty2 : 부족분 트리거에서 자동계산
            RequestQty :  사용자가 입력한 생산량
            ReservationStock : 사용자가 입력한 예약량
            ReservationStock_input : 사용자가 입력할 예약량인데 기본값은 가용재고, 수주량 중 작은 값
            RequestQty_input : 사용자가 입력한 생산필요량인데 기본값은 필요량 +(안전재고-(가용재고-예약량))-예약량
            mat_type :  product, semi, material
        */
		MapSqlParameterSource dicParam = new MapSqlParameterSource(); 
		dicParam.addValue("src_table", src_table);
		dicParam.addValue("src_data_pk", src_data_pk);
		dicParam.addValue("mat_type", mat_type);

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
	                left join unit u  on u.id = m."Unit_id" 
	            where  mr."MaterialType" = :mat_type
	            and mr."SourceDataPk" = :src_data_pk 
	            and mr."SourceTableName"= :src_table 
	              """;
        		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}
	
	// 제품 필요량 집계
	public boolean saveProdRequCalc(String src_table, Integer src_data_pk, String mat_type, Integer user_id) {
		
		// 기존 산출량 삭제
		List<MatRequ> mrList = this.matRequRepository.findBySourceTableNameAndSourceDataPkAndMaterialType(src_table, src_data_pk,  "product");
		
		for(int k = 0; k < mrList.size(); k++) {
			this.matRequRepository.deleteById(mrList.get(k).getId());
		}				

		this.matRequRepository.flush();
		
		// 산출량 저장
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("src_table", src_table);
		paramMap.addValue("src_data_pk", src_data_pk);
		paramMap.addValue("user_id", user_id);

		String sql = """
				with A as (
	                select s."Material_id" as product_id
				    , sum(s."SujuQty") as prod_qty
	                from suju s
	                where s."PlanTableName" = :src_table 
	                and s."PlanDataPk" = :src_data_pk
	                group by s."Material_id"
	                )
	            insert into mat_requ("MaterialType", "Material_id", "SourceTableName", "SourceDataPk"
	            , "RequireQty1", "RequireQty2"
	                ,"AvailableStock", "SafetyStock", "_created", "_modified", "_creater_id")
	            select 'product' 
	            , A.product_id
	            , :src_table as "SourceTableName"
	            , :src_data_pk as "SourceDataPk"
	            , A.prod_qty as req_qty1, A.prod_qty as req_qty2
	            , coalesce(m."AvailableStock", 0)
	            , m."SafetyStock"
	            , now() as "_created"
	            , now() as "_modified"
	            , :user_id as "_creater_id"
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

	// 제품 소요량 집계	
	public void saveMatRequCalc(String src_table, Integer src_data_pk, String mat_type, Integer user_id) {
		
		String materialType = "";
		if ("semi".equals(mat_type)) {
			materialType = "semi"; 
		} else if ("material".equals(mat_type)) {
			materialType = "material";
		}
		
		// 기존 산출량 삭제
		List<MatRequ> mrList = this.matRequRepository.findBySourceTableNameAndSourceDataPkAndMaterialType(src_table, src_data_pk, materialType);
		
		for(int k = 0; k < mrList.size(); k++) {
			this.matRequRepository.deleteById(mrList.get(k).getId());
		}

		this.matRequRepository.flush();
		
		// 산출량 저장
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("mat_type", mat_type);
		paramMap.addValue("src_table", src_table);
		paramMap.addValue("src_data_pk", src_data_pk);
		paramMap.addValue("user_id", user_id);

		String sql = """
				 with mreq as (
	                select mr."Material_id" as product_id
	                , mr."RequestQty" as prod_qty
	                from mat_requ mr
	                where mr."SourceTableName" = :src_table
	                and mr."SourceDataPk" = :src_data_pk
				""";
		
		if ("semi".equals(mat_type)) {
			sql += " and mr.\"MaterialType\" = 'product' "; 
		} else if ("material".equals(mat_type)) {
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
		
		if ("semi".equals(mat_type)) {
			sql += " and mg.\"MaterialType\" = 'semi' "; 
		} else if ("material".equals(mat_type)) {
			sql += """
					and mg."MaterialType" in ('raw_mat','sub_mat') /*원자재, 부자재*/
                    and bm.b_level = 1
					""";
		}
		
		sql += """
				group by bm.prod_pk, bm.mat_pk
	            )
	            insert into mat_requ("MaterialType", "Material_id", "SourceTableName", "SourceDataPk", "RequireQty1"
	                ,"AvailableStock", "SafetyStock", "_created", "_modified", "_creater_id")
	            select :mat_type as "MaterialType" 
	            , B.mat_pk as "Material_id"
	            , :src_table as "SourceTableName"
	            , :src_data_pk as "SourceDataPk"
	            --, sum(mreq.prod_qty * b.req_qty) as "RequireQty1"
	            , fn_unit_ceiling( sum(mreq.prod_qty * b.req_qty), u."PieceYN") as "RequireQty1"
	            , coalesce(m."AvailableStock", 0)
	            , m."SafetyStock"
	            , now() as "_created"
	            , now() as "_modified"
	            , :user_id as "_creater_id"
	            from B
	            inner join mreq on mreq.product_id=B.prod_pk
	            inner join material m on m.id = b.mat_pk
	            left join unit u on u.id = m."Unit_id"
	            group by B.mat_pk, m."AvailableStock",  m."SafetyStock", u."PieceYN"				
				""";
		
		
        this.sqlRunner.execute(sql, paramMap);  
	}
}
