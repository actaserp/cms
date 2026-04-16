package mes.app.definition;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.definition.service.StopCauseService;
import mes.domain.entity.StopCause;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.StopCauseRepository;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/definition/stop_cause")
public class StopCauseController {
	
	@Autowired
	private StopCauseService stopCauseService;
	
	@Autowired
	StopCauseRepository stopCauseRepository; 
	
	@Autowired
	SqlRunner sqlRunner;

	// 비가동사유 리스트 조회
	@GetMapping("/read")
	public AjaxResult getStopCauseList(
    		@RequestParam(value="plan_yn", required=false) String plan_yn,
    		@RequestParam(value="cause_name", required=false) String cause_name,
			HttpServletRequest request) {
		
		List<Map<String, Object>> items = this.stopCauseService.getStopCauseList(plan_yn,cause_name);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 비가동사유 상세정보 조회
	@GetMapping("/detail")
	public AjaxResult getStopCauseDetail(
    		@RequestParam(value="id", required=false) Integer id,
			HttpServletRequest request) {
		
		Map<String, Object> item = this.stopCauseService.getStopCauseDetail(id);      
        AjaxResult result = new AjaxResult();
        result.data = item;        
		return result;
	}
	
	// 비가동사유 삭제
	@PostMapping("/delete")
	public AjaxResult deleteStopCause(
    		@RequestParam(value="id", required=false) Integer id, 
    		HttpServletRequest request,
			Authentication auth) {
		      
        AjaxResult result = new AjaxResult();
        
        if(id != null) {
        	this.stopCauseRepository.deleteById(id);
        }
           
        result.success = true;        
		return result;
	}
	
	//비가동사유 상세정보 저장
	@PostMapping("/save")
	public AjaxResult saveStopCause (
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="stop_cause_name", required=false) String stop_cause_name,
			@RequestParam(value="plan_yn", required=false) String plan_yn,
			@RequestParam(value="description", required=false) String description,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		StopCause sc = null;
		
		if(id == null) {
			sc = new StopCause();
		} else {
			sc = this.stopCauseRepository.getStopCauseById(id);
		}
		
		boolean nameChk = this.stopCauseRepository.findByStopCauseName(stop_cause_name).isEmpty();
		
		if(stop_cause_name.equals(sc.getStopCauseName()) == false && nameChk == false) {
			result.success=false;
			result.message="중복된 비가동유형명이 존재합니다.";
			return result;	
		}
		
		sc.setStopCauseName(stop_cause_name);
		sc.setPlanYN(plan_yn);
		sc.setDescription(description);
		sc.setCoverage("all");
		sc.set_audit(user);
		this.stopCauseRepository.save(sc);
		
	    result.success=true;
	    return result;

	}
	
}
