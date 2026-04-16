package mes.app.haccp;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.haccp.service.DeviActionListService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/haccp/devi_action_list")
public class DeviActionListController {
	
	@Autowired
	private DeviActionListService deviActionListService;
	
	
	@GetMapping("/read")
	private AjaxResult getDeviActionList(
			@RequestParam("keyword") String keyword,
			@RequestParam("date_from") String dateFrom,
			@RequestParam("date_to") String dateTo) {
		
		List<Map<String, Object>> items = this.deviActionListService.getDeviActionList(keyword, dateFrom, dateTo);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}

}
