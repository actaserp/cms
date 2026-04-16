package mes.app.precedence.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class EquipRegisterService {

	@Autowired
	SqlRunner sqlRunner;
	
	// 설비목록 조회
	public List<Map<String, Object>> getEquipRegisterList(Integer area_id) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("area_id", area_id);
        
        String sql = """
        		select e.id as id, e."Code" as code, e."ManageNumber" as mng_number, e."Name" as equip_name
	            , e."SupplierName" as supplier_name, e."Maker" as maker, e."Voltage" as voltage
	            , e."DepartName" as dept_name, e."PurchaseCost" as pur_cost
		        , e."PurchaseDate" as pur_date, e."Usage" as usage, "PowerWatt" as power_watt
	            , e."ASTelNumber" as as_telnumber, e."Description" as description, e."AttentionRemark" as attention_remark
		        from equ e
		        left join work_center wc on wc.id = e."WorkCenter_id"
		        where 1 = 1
        		""";
  			    
	    if (area_id != null) {
	    	
	    	sql += " and wc.\"Area_id\" = :area_id ";
	    }
	    
	    sql += " order by e.\"Code\" ";
	    
	    List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        return items;
	}

	public Map<String, Object> getEquipRegisterDetailList(Integer id) {

		Map<String, Object> rtnMap = new HashMap<String, Object>();
		
		// 1. 설비 항목 조회
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("id", id);
        
        String sql = """
        		select e.id, e."Code"
	            , e."ManageNumber" as mng_number
	            , e."Name" as equip_name
	            , e."SupplierName" as supplier_name
	            , e."Maker", e."Voltage"
	            , e."DepartName", e."PurchaseCost"
		        , e."PurchaseDate" as pur_date
	            , e."Usage" as usage
	           , "PowerWatt" as power_watt
	           , e."ASTelNumber" as as_telnumber
	           , e."Description" as description
	           , e."AttentionRemark" as atten_remark
		        from equ e
	            where id = :id
        		""";
  			    	    
        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
        rtnMap.put("info", item);
                
        // 2. 이미지 조회
        sql = """
        		select af.id as file_id, af."AttachName" as attach_name, af."FileName" as "file_name" 
				from attach_file af
				where af."DataPk" = :id
				and af."TableName" = 'equ'
				--and af."AttachName" = 'basic'
				LIMIT 1
        		""";
        
        item = this.sqlRunner.getRow(sql, dicParam);
        rtnMap.put("photo", item);
        
        // 3. history 조회
        sql = """
        		select eh.id, eh."DataDate" as data_date, eh."Content" as content, eh."Description" as description
		        from equip_history eh
		        where eh."Equipment_id" = :id 
		        order by eh."DataDate"
        		""";
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        rtnMap.put("history", items);        
        
        return rtnMap;
	}
	
}
