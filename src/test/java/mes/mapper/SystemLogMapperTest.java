package mes.mapper;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import mes.app.system.service.SystemLogMapper;


@SpringBootTest
public class SystemLogMapperTest {

	@Autowired
	SystemLogMapper systemlogMapper;
	
	@Test
	public void logListTest() {
		
		List<Map<String, Object>> logs = this.systemlogMapper.getSystemLogList("2022-07-01", "2022-07-31", "error");
		System.out.println(logs);
	}
	
	@Test
	public void logDetailTest() {		
		Map<String, Object> log = this.systemlogMapper.getSystemLogDetail(100);
		System.out.println(log);
	}	
}
