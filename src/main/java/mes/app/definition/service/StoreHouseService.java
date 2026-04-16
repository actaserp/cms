package mes.app.definition.service;


import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;


import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class StoreHouseService {

	@Autowired
	SqlRunner sqlRunner;
	
	//창고 목록 조회
	public List<Map<String,Object>> getStorehouseList(String storehouseName,String spjangcd) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("storehouse_name", storehouseName);
		dicParam.addValue("spjangcd", spjangcd);
		
		String sql = """
			select sh.id
            , sh."Code" as storehouse_code
            , sh."Name" as storehouse_name
            , fn_code_name('storehouse_type', sh."HouseType") as storehouse_type
            , to_char(sh."_created" ,'yyyy-mm-dd') as created
            , f."Name" as factory_name
            , sh."Factory_id" as factory_id
            , sh."Description" as description
            from store_house sh
            left join factory f on f.id  = sh."Factory_id"
            where 1=1
            AND sh.spjangcd = :spjangcd
			""";
		if (StringUtils.isEmpty(storehouseName)==false) sql +="and upper(sh.\"Name\") like concat('%%',upper(:storehouse_name),'%%')";
			
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, dicParam);
		
		return items;
	}
	
	//창고 상세정보 조회
	public Map<String,Object> getStorehouseDetail(int storehouseId){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("storehouse_id", storehouseId);
		
		String sql = """
			select 
            sh.id, 
            sh."Code" as storehouse_code,
            sh."Name" as storehouse_name,
            sh."HouseType" as storehouse_type,
            to_char(sh."_created" ,'yyyy-mm-dd hh24:mi') as created,
            f."Name" as factory_name,
            sh."Factory_id" as factory_id,
            sh."Description" as description
            from store_house sh 
            left outer join factory f on sh."Factory_id"=f.id 
            where 1=1
            and sh.id = :storehouse_id
			""";
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
		
		return item;
	}
	
}
