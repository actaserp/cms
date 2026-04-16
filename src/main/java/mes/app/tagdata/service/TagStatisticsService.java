package mes.app.tagdata.service;

import java.util.List;
import java.util.Map;

import mes.app.common.TenantContext;
import org.apache.groovy.parser.antlr4.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class TagStatisticsService {
	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getStatisticsList(String start_date,String end_date,String tag_code,Integer tag_group_pk){
        String tenantId = TenantContext.get();
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("start_date", start_date);
        dicParam.addValue("end_date", end_date);
        dicParam.addValue("tag_code", tag_code);
        dicParam.addValue("tag_group_pk", tag_group_pk);
        dicParam.addValue("spjangcd", tenantId);
        
        String sql = """
			select td.tag_code, t.tag_name, count(*) as count_value, avg(td.data_value) as avg_value
                , min(td.data_value) as min_value, max(td.data_value) as max_value
                , round(stddev(td.data_value)::decimal,5) as std_value
	            from tag_dat td 
                inner join tag t on t.tag_code = td.tag_code
	            where td.data_date between TO_DATE(:start_date,'YYYY-MM-DD') and TO_DATE(:end_date,'YYYY-MM-DD') + interval '1 days'
	            and td.data_value is not null  
	            and td.spjangcd = :spjangcd
        """;
        
        if(StringUtils.isEmpty(tag_code) == false) {
        	sql+= " and td.tag_code = :tag_code ";
        }
        
        
        if(tag_group_pk != null) {
        	sql+= " and t.tag_group_id = :tag_group_pk ";
        }
        
        sql += "group by td.tag_code, t.tag_name";
        
        	
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
    };
}
