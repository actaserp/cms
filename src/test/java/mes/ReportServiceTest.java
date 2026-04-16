package mes;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import mes.domain.services.ReportService;

@SpringBootTest
public class ReportServiceTest {

	@Autowired
	ReportService reportService;
	
	@Test
	public void createTest() {
		
		//this.reportService.create(null, null);

	}
	
}
