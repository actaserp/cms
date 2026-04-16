package mes;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import mes.app.common.service.ApprResultService;

@SpringBootTest
public class ApprServiceTest {

	@Autowired	
	ApprResultService apprResultService;
	
	@Test
	public void init_appr_box() {
		
		int count = this.apprResultService.getApproverCheck(0, "haccp_diary", 0);
		System.out.println(count);
		
		if (count==0) {
			//http://localhost:9090/api/common/appr_result/init_appr_box?table=haccp_diary&task_code=내부검증결과보고서
			Map<String, Object> item = this.apprResultService.getApproverDefLine("내부검증결과보고서", null, "");
			System.out.println(item);
		}
		
		
		
	}
	
}
