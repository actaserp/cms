package mes.app.check;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.check.service.CheckMasterService;
import mes.domain.entity.CheckItem;
import mes.domain.entity.CheckMaster;
import mes.domain.entity.CheckResult;
import mes.domain.entity.CheckTarget;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.CheckItemRepository;
import mes.domain.repository.CheckMasterRepository;
import mes.domain.repository.CheckResultRepository;
import mes.domain.repository.CheckTargetRepository;

@RestController
@RequestMapping("/api/check/check_master")
public class CheckMasterController {
	
	@Autowired
	private CheckMasterService checkMasterService;

	@Autowired
	CheckMasterRepository checkMasterRepository;

	@Autowired
	CheckItemRepository checkItemRepository;

	@Autowired
	CheckTargetRepository checkTargetRepository;
	
	@Autowired
	CheckResultRepository checkResultRepository;
	
	// 점검등록 조회 	
	@GetMapping("/read")
	public AjaxResult getCheckMast(
    		@RequestParam(value="srch_check_name", required=false) String srch_check_name,
			HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.checkMasterService.getCheckMast(srch_check_name);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}

	// 점검등록 상세조회
	@GetMapping("/detail")
	public AjaxResult getCheckMastDetail(
    		@RequestParam(value="check_id", required=false) Integer check_id,
			HttpServletRequest request) {
		
        Map<String, Object> items = this.checkMasterService.getCheckMastDetail(check_id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}

	// 점검등록 저장
	@PostMapping("/save")
	public AjaxResult saveCheckMast(
			@RequestParam(value = "id", required = false) Integer id,
			@RequestParam(value = "check_name", required = false) String check_name,
			@RequestParam(value = "check_class_code", required = false) String check_class_code,
			@RequestParam(value = "code", required = false) String code,
			@RequestParam(value = "charger_department", required = false) String charger_department,
			@RequestParam(value = "check_cycle", required = false) String check_cycle,
			@RequestParam(value = "description", required = false) String description,
			@RequestParam(value = "direction", required = false) String direction,
			@RequestBody MultiValueMap<String,Object> dataList,
			HttpServletRequest request,
			Authentication auth) throws ParseException {
		
		AjaxResult result = new AjaxResult();
		User user = (User)auth.getPrincipal();

		CheckMaster checkMaster = null;
		
		if (id == null) {
			checkMaster = new CheckMaster();
		} else {
			checkMaster = this.checkMasterRepository.getCheckMasterById(id);
		}
		
		boolean check_code = this.checkMasterRepository.findByCode(code).isEmpty();
		
		if (code.equals(checkMaster.getCode()) == false && check_code == false) {
			result.success = false;
			result.message = "중복된 코드가 존재합니다.";
			return result;
		}
		
		checkMaster.setName(check_name);
		checkMaster.setCode(code);
		checkMaster.setCheckClassCode(check_class_code);
		checkMaster.setChargerDepart(charger_department);
		checkMaster.setCheckCycle(check_cycle);
		checkMaster.setDescription(description);
		checkMaster.set_audit(user);
		
		checkMaster = this.checkMasterRepository.save(checkMaster);
		
		result.data = checkMaster;
		
		return result;
	}

	@PostMapping("/delete")
	public AjaxResult deleteCheckMast(
			@RequestParam(value = "check_id", required = false) Integer check_id,
			@RequestBody MultiValueMap<String,Object> dataList,
			HttpServletRequest request,
			Authentication auth) throws ParseException {

		AjaxResult result = new AjaxResult();
		
		if (check_id != null) {

			List<CheckItem> check_item = this.checkItemRepository.findBycheckMasterId(check_id);
			
			if (check_item != null && check_item.size() > 0) {
				result.success = false;
				result.message = "해당 점검이 점검항목에서 사용중입니다.";
				return result;
			}
			
			List<CheckTarget> check_target = this.checkTargetRepository.findByCheckMasterId(check_id);
			
			if (check_target != null && check_target.size() > 0) {
				result.success = false;
				result.message = "해당 점검이 점검대상에서 사용중입니다.";
				return result;
			}
			
			List<CheckResult> check_result = this.checkResultRepository.findByCheckMasterId(check_id);

			if (check_result != null && check_result.size() > 0) {
				result.success = false;
				result.message = "해당 점검이 점검결과에서 사용중입니다.";
				return result;
			}
			
			this.checkMasterRepository.deleteById(check_id);			
		}		
		
		return result;
	}
	
}
