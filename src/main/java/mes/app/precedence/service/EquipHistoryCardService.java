package mes.app.precedence.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.entity.User;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@Service
public class EquipHistoryCardService {
	
	@Autowired
	SqlRunner sqlRunner;
	
	public Map<String, Object> getApprStat(String startDate, String endDate, String apprState) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("apprState", apprState);
		paramMap.addValue("startDate", startDate);
		paramMap.addValue("endDate", endDate);
		
		String sql = """
                select b.id, b."Char1" as "Title", COALESCE(r."State", 'write') as "State", COALESCE(r."StateName", '작성') as "StateName", r."LineName", r."LineNameState"
                , to_char(b."Date1", 'yyyy-MM-dd') as "DataDate", COALESCE(r."SearchYN", 'Y') as "SearchYN", COALESCE(r."EditYN", 'Y') as "EditYN", COALESCE(r."DeleteYN", 'Y') as "DeleteYN"                
                --, b."Char2"||' ~ '||b."Char3" as "DataDate"
                , coalesce(e.id, 0) as "EquipId", COALESCE(e."Name", ne."Name") as "EquipName", case when ne."Name" is not null then '신규설비' else '기존설비' end "EquipType"
                from bundle_head b
                left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
                left join equ e on b."Number1" = e.id
				left join equ ne on COALESCE(b."Char4", '0') = ne.id::text
                where b."TableName" = 'equip_history_card'
                --and (b."Char2" between :start_date and :end_date or b."Char3" between :start_date and :end_date)
                and b."Date1" between cast(:startDate as date) and cast(:endDate as date)
				""";
		
		if (!apprState.isEmpty() && apprState != null) {
            if (apprState.equals("write")) {
            	sql += " and r.\"State\" is null ";
            } else {
            	sql += " and r.\"State\" = :apprState ";
            }
		}
		
		sql += " order by b.\"Date1\" desc  ";
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		Map<String, Object> item = new HashMap<String, Object>();
		
		item.put("document_info", items);
		
		return item;
	}

	public Map<String, Object> getReadIn(Integer bh_id, Integer equ_id, String from_date, String to_date,User user) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bh_id", bh_id);
		paramMap.addValue("equ_id", equ_id);
		paramMap.addValue("from_date", from_date);
		paramMap.addValue("to_date", to_date);

		
		Map<String, Object> items = new HashMap<>();
		
		String sql = null;
		
		//일지 헤더
		Map<String, Object> head_info = new HashMap<>();
		//List<Map<String, Object>> new_equip = new ArrayList<Map<String, Object>>();
		Map<String, Object> new_equip = new HashMap<>();
		
		if (bh_id > 0) {
			sql = """
				    SELECT b.id, b."Char1" as "Title", COALESCE(r."State", 'write') as "State", COALESCE(r."StateName", '상신대기') as "StateName", r."LineName", r."LineNameState", COALESCE(uu."Name", cu."Name") as "FirstName"
					, to_char(b."Date1", 'yyyy-MM-dd') as "DataDate", COALESCE(r."SearchYN", 'Y') as "SearchYN", COALESCE(r."EditYN", 'Y') as "EditYN", COALESCE(r."DeleteYN", 'Y') as "DeleteYN"
					, b."Number1" as "EquipId", b."Char2" as "FromDate", b."Char3" as "ToDate", b."Char4" as "NewEquipId"
					FROM bundle_head b
					INNER JOIN user_profile cu ON b."_creater_id" = cu."User_id"
					LEFT JOIN user_profile uu ON b."_modifier_id" = uu."User_id"
					LEFT JOIN v_appr_result r ON b."id" = r."SourceDataPk" AND r."SourceTableName" = 'bundle_head'
					WHERE b."TableName" = 'equip_history_card'
					AND b."id" = :bh_id
				  """;
			
			head_info = this.sqlRunner.getRow(sql, paramMap);
						
		} else {
			head_info.put("id", 0);
			head_info.put("Title", "설비점검대장");
			head_info.put("DataDate", from_date);
			head_info.put("FirstName", user.getUserProfile().getName());
			head_info.put("State", "write");
			head_info.put("StateName", "작성");
			head_info.put("EquipId", equ_id);
			head_info.put("FromDate", from_date);
			head_info.put("ToDate", to_date);
			head_info.put("NewEquipId", 0);
						
		}
		
		sql = """
			SELECT eh.id
			, e.id AS equ_pk
			, e."Name" AS equ_name
			, eh."DataDate" AS data_date
			, eh."Content" AS content
			, eh."Description" AS description
			, eh."Cost" AS cost
			, eh."Char1" AS manager
			, eh."Char2" AS part_leader
			FROM equip_history eh
			INNER JOIN equ e ON e.id = eh."Equipment_id"
			WHERE e.id = :equ_id
			    AND eh._status = 'history'
			    AND (
			        (
			            0 = :bh_id
			            AND COALESCE(eh."SourceDataPk"::text , '') = ''
			            AND eh."DataDate" BETWEEN CAST(:from_date AS timestamp) AND CAST(:to_date AS timestamp) + interval '0.99999 seconds'
			        )
			        OR (
			            eh."SourceTableName" = 'bundle_head'
			            AND eh."SourceDataPk" = :bh_id
			        )
			    )
			ORDER BY eh._created desc
			""";
		
		List<Map<String, Object>> repair_info = this.sqlRunner.getRows(sql, paramMap);
		
		sql = """
				select eh.id, to_char(eh._created, 'yyyy-MM-dd HH:mm:ss') as "ChangeDate", u."Name" as "ChangeName", eh."Text1"
                from equip_history eh
                inner join equ e on e.id = eh."Equipment_id"
				left join user_profile u on eh._creater_id = u."User_id"
                where e.id = :equ_id
				and eh._status = 'updateHis'
				and (
					(
					    0 = :bh_id
                        and COALESCE(eh."SourceTableName", '')=''
			            AND eh."DataDate" BETWEEN CAST(:from_date AS timestamp) AND CAST(:to_date AS timestamp) + interval '0.99999 seconds'
                    )
					or (
						eh."SourceTableName" = 'bundle_head'
						and eh."SourceDataPk" = :bh_id
					)
				)
                order by eh._created desc
				""";
			
		List<Map<String, Object>> history_info = this.sqlRunner.getRows(sql, paramMap);
		
		Integer NewEquipId = CommonUtil.tryIntNull(head_info.get("NewEquipId"));
		
		if(NewEquipId != null && NewEquipId > 0) {
			paramMap.addValue("NewEquipId", CommonUtil.tryIntNull(head_info.get("NewEquipId")));
			
			sql = """
			    select e.id
	             , e."Code"
	             , e."Name"
	             , e."Description"
	             , e."Maker"
	             , e."Model"
	             , e."ManageNumber"
	             , e."SerialNumber"
	             , e."SupplierName"
	             , e."ProductionYear"
	             , to_char(e."PurchaseDate",'yyyy-mm-dd') as "PurchaseDate"
	             , e."Manager"
	             , e."PurchaseCost" 
	             , e."ServiceCharger"
	             , e."InstallDate"
	             , e."DisposalDate"
	             , e."OperationRateYN"
	             , e."Status"
	             , e."EquipmentGroup_id"
	             , eg."Name" as group_name
	             , e."WorkCenter_id"
	             , wc."Name" as workcenter_name
	             , e."Depart_id"
	             , d."Name" as DepartName
	             , to_char(e._created ,'yyyy-mm-dd hh24:mi') as _created 
	             , e."Inputdate", e."Voltage", e."Usage", "PowerWatt", e."ASTelNumber", e."AttentionRemark"
	             , e."OvenTemperCount"
	             , e."OvenProductTemperStandard"
	             , e."OvenHeatingMnStandard"
	             , e."ForeignDetectMaster_id"
	             , e."OvenCount"
	             , e."CcpTestCycle"
	             , pd."Char1" as "Located"
	             , COALESCE(e._status, 'A') as equ_status
	             , case COALESCE(e._status, 'A') when 'A' then '사용' when 'D' then '폐기' else '대기' end equ_status_name
	             from equ e
	              left join equ_grp eg on e."EquipmentGroup_id" =eg.id 
	              left join work_center wc  on wc.id = e."WorkCenter_id"   
	              left join depart d on d.id = e."Depart_id"
	              left join prop_data pd on pd."DataPk"=e.id and pd."TableName"='equ' and pd."Code"='located'
	             where e.id = :NewEquipId
				  """;
			
			new_equip = this.sqlRunner.getRow(sql, paramMap);
		}
			
		items.put("head_info",head_info);
		items.put("repair_info",repair_info);
		items.put("history_info",history_info);
		items.put("new_equip", new_equip);
		
		return items;
	}

	public void deleteEquipHistoryAndEqu(Integer bh_id) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bh_id", bh_id);
		
		String sql = """
                delete from equip_history where "Equipment_id" in (select "Char4"::int from bundle_head where id = :bh_id)
				""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
                delete from equ where id in (select "Char4"::int from bundle_head where id = :bh_id)
				""";
		
		this.sqlRunner.execute(sql, paramMap);
	}
	
	public void updateEquipHistoryAndEqu(Integer bh_id) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bh_id", bh_id);
		
		String sql = """
                update equip_history
	            set "SourceDataPk" = b.id
		            , "SourceTableName" = 'bundle_head'
	            from bundle_head b
	            where b.id = :bh_id
	            and equip_history."Equipment_id" = b."Char4"::int
				""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
                update equ 
	            set _status = 'A'
	            where id in (select "Char4"::int from bundle_head where id = :bh_id)
				""";
		
		this.sqlRunner.execute(sql, paramMap);
	}
}
