package mes.app.haccp.mat_inout_stock.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class MatInoutStockService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getMatInoutStockList(String startDate, String endDate) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("startDate", startDate);
		paramMap.addValue("endDate", endDate);
		
		String sql = """
				select b.id, b."Char1" as "Title", coalesce(r."StateName", '작성') as "StateName"
				, r."LineName", r."LineNameState", to_char(b."Date1", 'yyyy-MM-dd') as "DataDate"
				, coalesce(r."SearchYN", 'Y') as "SearchYN", coalesce(r."EditYN", 'Y') as "EditYN"
				, coalesce(r."DeleteYN", 'Y') as "DeleteYN", b."Number1" as check_master_id
				, b._creater_id ,up."Name" as "creater_name" , b._modifier_id, up2."Name" as "modifier_name"
				from bundle_head b                               
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				left join user_profile up on b._creater_id = up."User_id"
				left join user_profile up2 on b._modifier_id = up2."User_id"
				where b."TableName" = 'mat_inout_stock_result'
				and b."Date1" between cast(:startDate as date) and cast(:endDate as date)
				""";
		
		sql += " order by b.\"Date1\" desc ";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public Map<String, Object> getResultList(Integer bhId) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		
		String sql = null;
		
		sql = """
				select b.id, b."Char1" as title, to_char(b."Date1", 'yyyy-MM-dd') as "DataDate"
				, b."Number1" as "housePk", b."Char2" as "startDt", b."Char3" as "endDt"
                from bundle_head b
				inner join user_profile cu on b._creater_id = cu."User_id"
				left join user_profile uu on b._modifier_id = uu."User_id"
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				where b."TableName" = 'mat_inout_stock_result'
                and b.id = :bhId
				""";
		
		Map<String,Object> mstInfo = this.sqlRunner.getRow(sql, paramMap);
		
		
		
		sql = """
					select mi.id as mio_pk
                    , fn_code_name('inout_type', mi."InOut") as inout
                    , case when mi."InOut" = 'in' then fn_code_name('input_type', mi."InputType") 
	                    when mi."InOut" = 'out' then fn_code_name('output_type', mi."OutputType") end as inout_type
                    , to_char(mi."InoutDate",'yyyy-mm-dd ') as "InoutDate"
                    , to_char(mi."InoutTime", 'hh24:mi') as "InoutTime"
                    , m."Code" as "material_code"
                    , m."Name" as "material_name"
                    , mih2."CurrentStock" as "HouseStock"
                    , coalesce(mi."InputQty", 0) as "InputQty"
                    , coalesce(mi."OutputQty", 0) as "OutputQty"
                    , u2."Name" as "unit_name"
                    , mi."Description"
                    , fn_code_name('mat_type', mg."MaterialType") as material_type
                    , mr."Char1" as result1
                    from master_result mr 
                    inner join mat_inout mi on mi.id = mr."MasterTable_id"
                    inner join bundle_head bh on mr."SourceDataPk"  = bh.id
                    inner join material m on mi."Material_id" = m.id
                    left join mat_grp mg on mg.id = m."MaterialGroup_id"
                    inner join store_house sh on mi."StoreHouse_id" = sh.id
                    left join unit u2 on m."Unit_id" = u2.id 
                    left join mat_in_house mih2 on mih2."Material_id"  = m.id
                    and mih2."StoreHouse_id" = mi."StoreHouse_id"
                    where 1 = 1
                    and bh."TableName" = 'mat_inout_stock_result'
                    and bh.id = :bhId
			  """;
			
		List<Map<String,Object>> itemResult = this.sqlRunner.getRows(sql, paramMap);
		
		sql = """
                select 
                	mi.id mio_pk
                	, da.id
		            , da."HappenDate" as happen_date, da."HappenPlace" as happen_place
		            , da."AbnormalDetail" as abnormal_detail, da."ActionDetail" as action_detail
		            , da."ActionState" as action_state, da."ConfirmState" as confirm_state
		            , da."SourceDataPk" as src_data_pk, da."SourceTableName" as src_table_name
		            , da."ConfirmDetail" as confirm_detail 
		            , up."Name" as actor_name
		            , up2."Name" as creater_name
		            , RIGHT(da."AbnormalDetail", POSITION(':' in REVERSE(da."AbnormalDetail"))-1) as check_name
				    , ROW_NUMBER() OVER (ORDER BY mr.id) AS _order
	            from devi_action da
	            inner join master_result mr on mr.id = da."SourceDataPk" and da."SourceTableName" = 'mat_inout_stock_result'
                inner join mat_inout mi on mr."MasterTable_id"  = mi.id
	            and mr."SourceDataPk" = :bhId
	            left join user_profile up on up."User_id" = da._modifier_id
	            left join user_profile up2 on up2."User_id" = da._creater_id
                order by mr.id desc
			  """;
		
		List<Map<String, Object>> itemDeviResult = this.sqlRunner.getRows(sql, paramMap);
		
		Map<String, Object> item = new HashMap<String, Object>();
		
		item.put("mst_info", mstInfo);
		item.put("item_result", itemResult);
		item.put("item_devi_result", itemDeviResult);
		
		return item;
	}
	@Transactional
	public void mstDelete(Integer bhId) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		
		String sql = null;
		
		sql = """
            delete from master_result                     
            where "SourceDataPk" = :bhId
			""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
		    delete from devi_action
                where "SourceDataPk" not in (
	                select a.id
	                from master_result a
	                left join  devi_action b on b."SourceDataPk" = a.id
	                where 1=1
	                and "Char1" = 'X'
                )
                and "SourceTableName" = 'mat_inout_stock_result'
			  """;
			
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
				delete from bundle_head where id = :bhId
			  """;
			
		this.sqlRunner.execute(sql, paramMap);
	}

}
