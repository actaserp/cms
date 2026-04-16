package mes.app.dashboard;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.dashboard.service.DashBoardService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/dashboard")
public class DashBoardController {
	
	@Autowired
	private DashBoardService dashBoardService;
	
	@GetMapping("/today_week_prod")
	private AjaxResult todayWeekProd(
			@RequestParam("spjangcd") String spjangcd
	) {
		
		List<Map<String, Object>> items = this.dashBoardService.todayWeekProd(spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}

	@GetMapping("/today_prod")
	private AjaxResult todayProd(
			@RequestParam("spjangcd") String spjangcd
	) {
		
		List<Map<String, Object>> items = this.dashBoardService.todayProd(spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/year_def_prod")
	private AjaxResult yearDefProd(
			@RequestParam("spjangcd") String spjangcd
	) {
		
		List<Map<String, Object>> items = this.dashBoardService.yearDefProd(spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/mat_stock")
	private AjaxResult matStock(
			@RequestParam("spjangcd") String spjangcd
	) {
		
		List<Map<String, Object>> items = this.dashBoardService.matStock(spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/custom_order")
	private AjaxResult customOrder(
			@RequestParam("spjangcd") String spjangcd
	) {
		
		List<Map<String, Object>> items = this.dashBoardService.customOrder(spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/custom_service_stat")
	private AjaxResult customServiceStat(
			@RequestParam(value="dateType", required=false) String dateType) 
	{
		Map<String, Object> items = this.dashBoardService.customServiceStat(dateType);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/custom_service_stat_result")
	private AjaxResult customServiceStatResult() 
	{
		List<Map<String, Object>> items = this.dashBoardService.customServiceStatResult();
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	
	
	@GetMapping("/haccp_read")
	private AjaxResult haccp_read(
			@RequestParam("year_month") String year_month,
			@RequestParam("data_year") String data_year,
			@RequestParam("data_month") String data_month,
			Authentication auth,
			HttpServletRequest request
			) {
		
		Map<String, Object> items = this.dashBoardService.haccpReadResult(year_month,data_year,data_month,auth);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
		
		
	@GetMapping("/getCppList")
	private AjaxResult getCppList(
			@RequestParam("strDate") String strDate,
			Authentication auth,
			HttpServletRequest request
			) {
		
		Map<String, Object> items = this.dashBoardService.getCppList(strDate,auth);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	
	
	@GetMapping("/detail_haccp_process")
	private AjaxResult detail_haccp_process(
			Authentication auth,
			HttpServletRequest request
			) {
		
		Map<String, Object> haccpProcessItems = this.dashBoardService.getDetailHacpPro();
		
		AjaxResult result = new AjaxResult();
		result.data = haccpProcessItems;
		
		return result;
	}

	
	
	
}
