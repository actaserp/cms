package mes;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import mes.domain.services.ComboService;
import mes.domain.services.SqlRunner;

@SpringBootTest
public class ComboServiceTest {
	
	@Autowired
	ComboService comboService;

	@Autowired
	SqlRunner sqlRunner;	
	
	@Test
	public void testGetComboList() {		
		List<Map<String, Object>> data = this.comboService.getComboList("system_code", "company_type", null, null);
		System.out.println(data);
		
		List<Map<String, Object>> data2 = this.comboService.area.getDataList(null, null, null);
		System.out.println(data2); // 데이터가 없다???
		
		List<Map<String, Object>> items = this.comboService.getComboList("haccp_process", "ht", null, null);
		System.out.println(items);		
		
		items = this.comboService.getComboList("haccp_process", "fsd", null, null);
		System.out.println(items);
		
		
		String sql = """		
				select id as value, \"Name\" as text from haccp_proc where 1=1
				and \"ProcessKind\"=:cond1
				order by \"Name\" 
			    """;
				
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("cond1", "haccp_process");
		items = this.sqlRunner.getRows(sql, dicParam);
		        
		System.out.println(items);		        
	}	
}


