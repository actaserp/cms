package mes.app.definition.service;

import mes.app.common.TenantContext;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;

@Service
public class FactoryService {

    @Autowired
    SqlRunner sqlRunner;

    public List<Map<String, Object>> getFactory(String keyword, String spjangcd) {

        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("keyword", keyword);
        paramMap.addValue("spjangcd", spjangcd);

        String sql = """
				SELECT d.id
		        , d."Name" as dept_name
		        , d."Code" as dept_code
		        FROM factory d
		        where 1=1
		        AND d.spjangcd = :spjangcd
				""";
        if(StringUtils.hasText(keyword)) {
            sql += """
					and ( d."Name" like concat('%%',:keyword,'%%')
				     	or d."Code" like concat('%%',:keyword,'%%')
				     	)
					""";
        }

        sql += " order by d.\"Name\" ";

        List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
    }

    public Map<String, Object> getFactoryDetail(int id) {
			String tenantId = TenantContext.get();
        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("id", id);
				paramMap.addValue("spjangcd", tenantId);

        String sql = """
				SELECT d.id
		        , d."Name" 
		        , d."Code"
		        FROM factory d
	            where d.id = :id
	            and d.spjangcd = :spjangcd
				""";


        Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);

        return item;
    }

}
