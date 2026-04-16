package mes.app.system.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class PropertyMasterService {

	@Autowired
	SqlRunner sqlRunner;	
	
	// 프로퍼티 리스트 조회
	public List<Map<String, Object>> getPropertyMasterList(String tb_name){
		MapSqlParameterSource dicParam = new MapSqlParameterSource(); 
		dicParam.addValue("tb_name", tb_name);

        String sql = """
		   select pm.id
            , fn_code_name('prop_master', pm."TableName") as table_name
            , pm."Code" as table_code
            , fn_code_name('value_type', pm."Type") as val_type
            , pm."Description" as description
            from prop_master pm
            left join sys_code sc on pm."TableName" =sc."Value" 
            where 1=1 
            """;
        
        if (StringUtils.isEmpty(tb_name) == false)
        	sql += " and pm.\"TableName\"  = :tb_name ";
        		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}

	// 프로퍼티 상세 조회
	public Map<String, Object> getPropertyMasterDetail(Integer id){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("prop_master_id", id);
        
        String sql = """
		    select pm.id
            , pm."TableName" as table_name
            , pm."Code" as table_code
            , pm."Type" as val_type
            , pm."Description" as description
            from prop_master pm 
            where pm.id = :prop_master_id
		    """;
        
        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
        
        return item;
	}
	
}
