package mes.app.test.service;

import java.util.List;
import java.util.Map;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class TestMasterGroupService {
	
	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getTestMasterGroupList(String testGroupName, String testClass) {
		String tenantId = TenantContext.get();
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("test_grp_name", testGroupName);
		dicParam.addValue("test_class", testClass);
		dicParam.addValue("spjangcd", tenantId);

		String sql = """
			select tmg.id 
            , tmg."Name" as test_grp_name
            , tmg."TestClass" as test_class
            from test_mast_grp tmg 
            where 1=1
            and tmg.spjangcd = :spjangcd
			""";

		if (StringUtils.isEmpty(testGroupName) == false)
			sql += " and tmg.\"Name\" like concat('%%',:test_grp_name,'%%')";
		if (StringUtils.isEmpty(testClass) == false)
			sql += " and tmg.\"TestClass\" = :test_class";

		sql += " order by tmg.id";


		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		
		return items;	
		
	}
	
}