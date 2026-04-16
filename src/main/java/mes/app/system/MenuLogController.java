package mes.app.system;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.system.service.MenuLogService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/system/menulog")
public class MenuLogController {

	@Autowired 
	private MenuLogService menuLogService;
	
	@GetMapping("/log_count")
	public AjaxResult getLogCount(
			@RequestParam("date_from") String dateFrom,
			@RequestParam("date_to") String dateTo,
			@RequestParam("cboMenu") String menuCode,
			@RequestParam("cboUser") String userPk,
			@RequestParam("spjangcd") String spjangcd){
		
		List<Map<String, Object>> items = this.menuLogService.getLogCount(dateFrom,dateTo,menuCode,userPk, spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/log_list")
	public AjaxResult getLogList(
			@RequestParam("date_from") String dateFrom,
			@RequestParam("date_to") String dateTo,
			@RequestParam("cboMenu") String menuCode,
			@RequestParam("cboUser") String userPk,
			@RequestParam("spjangcd") String spjangcd){
		
		List<Map<String, Object>> items = this.menuLogService.getLogList(dateFrom,dateTo,menuCode,userPk, spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	// 사용자 목록 조회
	@GetMapping("/user_list")
	public AjaxResult getUserList(
			@RequestParam("spjangcd") String spjangcd
	){
		
		List<Map<String, Object>> items = this.menuLogService.getUserList(spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
}
