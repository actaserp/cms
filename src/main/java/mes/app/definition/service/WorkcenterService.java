package mes.app.definition.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class WorkcenterService {
	
	@Autowired
	SqlRunner sqlRunner;
	
	// 워크센터 목록 조회
	public List<Map<String, Object>> getWorkcenterList(String keyword, String spjangcd){
		MapSqlParameterSource dicParam = new MapSqlParameterSource(); 
		dicParam.addValue("keyword", keyword);
		dicParam.addValue("spjangcd", spjangcd);


        String sql = """
			select wc.id, wc."Code" as code
                , wc."Name" as name
                , fa."Name" as factory_name
                , a."Name" as area_name
                , wc."HierarchyLevel" as hierarchy_level
                , fn_code_name('hierarchy_level',  wc."HierarchyLevel" ) as hierarchy_level_name
                , wc."OutSourcingYN" as outsourcing_yn
                , p."Name" as "ProcessName"
                , wc."Process_id"
                , sh."Name" as process_storehouse_id
                from work_center wc
                left join factory fa on fa.id = wc."Factory_id"
                left join area a on a.id = wc."Area_id"
                left join process p on p.id= wc."Process_id"
                left join store_house sh on sh.id = wc."ProcessStoreHouse_id"
                where 1=1 
                AND wc.spjangcd = :spjangcd
            """;
        if (StringUtils.isEmpty(keyword)==false) sql +="and upper(wc.\"Name\") like concat('%%',upper( :keyword ),'%%')";
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}
	
	// 워크센터 상세정보 조회
	public Map<String, Object> getWorkcenterDetail(int workcenterId){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("workcenterId", workcenterId);
        
        String sql = """
			select wc.id
            , wc."Code" as code
            , wc."Name" as name
            , wc."Factory_id" as factory_id
            , wc."Area_id" as area_id
            , wc."HierarchyLevel" as hierarchy_level
            , wc."OutSourcingYN" as outsourcing_yn
            , wc."Process_id"
            , p."Name" as "ProcessName"
            , wc."ProcessStoreHouse_id" as process_storehouse_id
            from work_center wc
            left join process p on p.id= wc."Process_id"
            where wc.id=:workcenterId
		    """;
        
        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
        
        return item;
	}
	
	// 설비정보 조회
	public List<Map<String, Object>> getEquipmentList(int workcenterId){
			
			MapSqlParameterSource dicParam = new MapSqlParameterSource();        
	        dicParam.addValue("workcenterId", workcenterId);
	    
	    String sql = """
			select 
              rd.id
              , rd."DataPk1" as "WorkCenter_id"
              , rd."DataPk2" as "Equipment_id"
              , wc."Name" as "WorkCenterName"
              , e."Name" as "EquipmentName"
              , e."Code" as "EquipmentCode"
              , to_char(rd._created ,'yyyy-mm-dd hh24:mi:ss') as _created 
            from rela_data rd 
                left join work_center wc on wc.id = rd."DataPk1"
                left join equ e on e.id = rd."DataPk2"
            where rd."TableName1"='work_center' and rd."TableName2"='equ'
            and rd."DataPk1" = :workcenterId
		    """;
	    
	    List<Map<String, Object>> item = this.sqlRunner.getRows(sql, dicParam);
	    
	    return item;
	}
	
	// 저장된 설비정보 체크
	public boolean checkEquipment (Integer workcenter_id, Integer equipment_id) {
		boolean result = false;
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		
		paramMap.addValue("workcenter_id", workcenter_id);
		paramMap.addValue("equipment_id", equipment_id);
		
		String sql = """
				select * from rela_data
                where "TableName1"='work_center'
                and "TableName2"='equ'
                and "DataPk1" = :workcenter_id
                and "DataPk2" = :equipment_id
                and "RelationName" = 'workcenter-equipment'
			    """;
		
		Map<String, Object> map = this.sqlRunner.getRow(sql, paramMap);
		
		if(map!=null) {
			result = true;
		}
		return result;
	}

}
