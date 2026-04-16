package mes.app.schedule;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.schedule.service.MatOrderListService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/schedule/mat_order")
public class MatOrderListController {

	@Autowired
	private MatOrderListService matOrderListService;

	// 자재발주내역 조회
	@GetMapping("/mat_order_history_list")
	public AjaxResult getMatOrderHistorylist(
			@RequestParam(value="date1", required=true) String date1,
			@RequestParam(value="date2", required=true) String date2,
			@RequestParam(value="date_kind", required=false) String date_kind,
			@RequestParam(value="company_id", required=false) Integer company_id,
			@RequestParam(value="mat_group_id", required=false) Integer mat_group_id,
			@RequestParam(value="mat_id", required=false) Integer mat_id,			
			HttpServletRequest request) {

		List<Map<String, Object>> items = this.matOrderListService.getMatOrderHistorylist(date_kind, date1, date2, company_id, mat_group_id, mat_id);
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}
		
}
