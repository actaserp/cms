package mes.app.tagdata.service;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class TagDataService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getTagDataList(String data_from,String data_to,String tag_code){
		String tenantId = TenantContext.get();
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("data_from", Timestamp.valueOf(data_from));
        dicParam.addValue("data_to", Timestamp.valueOf(data_to));
        dicParam.addValue("tag_code", tag_code);
		dicParam.addValue("spjangcd", tenantId);
        String sql = """
			select td.tag_code as tag_code
		        ,t.tag_name as tag_name
		        ,to_char(td.data_date, 'yyyy-mm-dd hh24:mi:ss') as data_date
                , td.data_value
                , td.data_char
	            from tag_dat td
                inner join tag t on t.tag_code = td.tag_code
	            where 1=1
                and td.tag_code = :tag_code
	            and td.data_date between :data_from and :data_to
	            and td.spjangcd = :spjangcd
                order by td.tag_code, td.data_date  
        """;
        	
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
    };
}
