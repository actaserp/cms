package mes.app.check;

import java.sql.Date;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.check.service.CheckResultService;
import mes.domain.entity.CheckItemResult;
import mes.domain.entity.CheckResult;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.CheckItemResultRepository;
import mes.domain.repository.CheckResultRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/check/check_result")
public class CheckResultController {
	
	@Autowired
	private CheckResultService checkResultService;

	@Autowired
	private CheckResultRepository checkResultRepository;

	@Autowired
	private CheckItemResultRepository checkItemResultRepository;
	
	@Autowired
	SqlRunner sqlRunner;
	
	// 점검이력 - 조회
	@GetMapping("/read")
	public AjaxResult getCheckResult(
    		@RequestParam(value="check_master_id", required=false) Integer check_master_id, 
    		@RequestParam(value="check_class_code", required=false) String check_class_code, 
    		@RequestParam(value="date_from", required=false) String date_from, 
    		@RequestParam(value="date_to", required=false) String date_to,
			HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.checkResultService.getCheckResult(check_master_id, check_class_code, date_from, date_to);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 점검이력 - 상세조회
	@GetMapping("/detail")
	public AjaxResult getCheckResultDetail(
    		@RequestParam(value="check_result_id", required=false) Integer check_result_id,
			HttpServletRequest request) {
		
        Map<String, Object> items = this.checkResultService.getCheckResultDetail(check_result_id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 점검표 - 조회
	@GetMapping("/check_item_result_list")
	public AjaxResult getCheckItemResultlist(
    		@RequestParam(value="check_result_id", required=false) Integer check_result_id,
			HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.checkResultService.getCheckItemResultlist(check_result_id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}

	// 점검이력 - 추가
	@PostMapping("/insert")
	public AjaxResult insertCheckResult(
			@RequestParam(value = "check_result_id", required = false) Integer check_result_id,
			@RequestParam(value = "check_name", required = false) Integer check_name,
			@RequestParam(value = "check_date", required = false) String check_date,
    		@RequestParam(value = "check_time", required=false) String check_time,
    		@RequestParam(value = "target_name", required=false) String target_name,
    		@RequestParam(value = "checker_name", required=false) String checker_name,
			HttpServletRequest request,
			Authentication auth) throws ParseException {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		CheckResult cr = new CheckResult();
		cr.setCheckMasterId(check_name);
        cr.setCheckDate(Date.valueOf(check_date));    
        cr.setCheckTime(check_time);
        cr.setTargetName(target_name);  
        cr.setCheckerName(checker_name);
        cr.set_audit(user);
        
        cr = this.checkResultRepository.save(cr);
        
        check_result_id = cr.getId();
        
        MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("check_result_id", check_result_id);
		paramMap.addValue("check_name", check_name);
		
		String sql = """
				insert into check_item_result("CheckResult_id", "CheckItem_id", _order,_created)
                select :check_result_id, ci.id, ci._order , now()
                from check_item ci
                where ci."CheckMaster_id" = :check_name
                and current_date between ci."StartDate" and ci."EndDate"
				""";
		
	    this.sqlRunner.execute(sql, paramMap);
		
		return result;
	}
	
	// 점검표 - 저장
	@PostMapping("/save")
	public AjaxResult saveCheckResult(
			@RequestParam(value = "check_result_id", required = false) Integer check_result_id,
			@RequestParam(value = "description", required = false) String description,
			@RequestBody MultiValueMap<String,Object> dataList,
			HttpServletRequest request,
			Authentication auth) throws ParseException {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
	    List<Map<String, Object>> items = CommonUtil.loadJsonListMap(dataList.getFirst("Q").toString());

		if (items.size() == 0) {
			result.success = false;
			return result;
		}
		
		for (int i = 0; i < items.size(); i++) {

			Integer pk = Integer.parseInt(items.get(i).get("id").toString());
			String result1 = (String) items.get(i).get("result1");

			CheckItemResult cir = this.checkItemResultRepository.getCheckItemResultById(pk);

			cir.setResult1(result1);
	        cir.set_audit(user);
	        
	        cir = this.checkItemResultRepository.save(cir);			
		}
		
		CheckResult cr = this.checkResultRepository.getCheckResultById(check_result_id);
		
		cr.setDescription(description);
		cr = this.checkResultRepository.save(cr);
		        
        MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("check_result_id", check_result_id);
		paramMap.addValue("user_id", user.getId());
		
		String sql = """
                call make_devi_action_with_check_result(:check_result_id, :user_id)
				""";
		
	    this.sqlRunner.execute(sql, paramMap);
		
		return result;
	}
	
	// 점검이력 - 삭제
	@PostMapping("/delete")
	public AjaxResult deleteCheckResult(
			@RequestParam(value = "check_result_id", required = false) Integer check_result_id,
			HttpServletRequest request) throws ParseException {

			AjaxResult result = new AjaxResult();
			
			if (check_result_id != null) {
				
				List<CheckItemResult> cirList = this.checkItemResultRepository.findByCheckResultId(check_result_id);
				
				for(int i = 0; i < cirList.size(); i++) {
					this.checkItemResultRepository.deleteById(cirList.get(i).getId());
				}				
				
				this.checkResultRepository.deleteById(check_result_id);
			} else {
				result.success = false;
			}
			
			return result;
	}	
	
	@GetMapping("/check_devi_action_list")
	public AjaxResult checkDeviActionList(
			@RequestParam(value = "data_pk", required = false) Integer dataPk) {
		
        List<Map<String, Object>> items = this.checkResultService.checkDeviActionList(dataPk);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
}
