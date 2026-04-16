package mes;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import mes.domain.entity.SystemLog;
import mes.domain.repository.SystemLogRepository;
import mes.domain.services.SqlRunner;

@SpringBootTest
class MesApplicationTests {

	@Autowired
	SystemLogRepository systemLogRepository;
	
	@Autowired
	SqlRunner sqlRunner;
	
	@Test
	void sqlRuunerTest() {
		
		List<Map<String, Object>> items;		
		String sql = """
		select * from job_res order by id desc limit 1				
		""";
		
		items =  this.sqlRunner.getRows(sql, null);		
		items.forEach(row -> {
			System.out.println(row);
			System.out.println(row.get("WorkOrderNumber"));
			System.out.println(row.get("ProductionDate"));
			System.out.println(row.get("GoodQty"));
			Object qty = row.get("GoodQty");
			
			Integer  Parent_id = (Integer)row.get("Parent_id");
			System.out.println(Parent_id);
			// null 일 경우 캐스팅 오류
			//int pid = (int)row.get("Parent_id");
			
			
			//GoodQty--> double
			System.out.println(qty.getClass());			
		});	
	}
	
	@Test
	void repositoryTest() {
		Optional<SystemLog> optSystemLog =  this.systemLogRepository.findById(1L);
		System.out.println(optSystemLog);
		if( optSystemLog.isEmpty()==false) {
			SystemLog systemLog = optSystemLog.get();
			System.out.println(systemLog.getId());
			System.out.println(systemLog.getMessage());
		}
		
	}

}
