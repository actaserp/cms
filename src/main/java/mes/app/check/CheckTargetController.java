package mes.app.check;

import java.sql.Date;
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

import io.micrometer.core.instrument.util.StringUtils;
import mes.app.check.service.CheckTargetService;
import mes.domain.entity.CheckTarget;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.CheckTargetRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.DateUtil;

@RestController
@RequestMapping("/api/check/check_target")
public class CheckTargetController {

	@Autowired
	private CheckTargetService checkTargetService;

	@Autowired
	CheckTargetRepository checkTargetRepository;
	
	// 점검대상 조회 	
	@GetMapping("/read")
	public AjaxResult getCheckTarget(
    		@RequestParam(value="check_master_id", required=false) Integer check_master_id, 
    		@RequestParam(value="check_date", required=false) String check_date,
			HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.checkTargetService.getCheckTarget(check_master_id, check_date);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}

	// 점검대상 상세조회
	@GetMapping("/detail")
	public AjaxResult getCheckTargetDetail(
    		@RequestParam(value="id", required=false) Integer id,
			HttpServletRequest request) {
		
        Map<String, Object> items = this.checkTargetService.getCheckTargetDetail(id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}

	// 순서 저장
	@PostMapping("/saveIndex")
	public AjaxResult saveCheckTargetIndex(
			@RequestParam(value = "to_storehouse_id", required = false) Integer to_storehouse_id,
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
		
		Integer index = 1;
		
		for (int i = 0; i < items.size(); i++) {
			
			Integer pk = Integer.parseInt(items.get(i).get("id").toString());
			
			CheckTarget jr = this.checkTargetRepository.getCheckTargetById(pk);
			
			if(jr != null) {
				jr.setOrder(index);
				jr.set_audit(user);
				jr = this.checkTargetRepository.save(jr);
			}			
			
			index = index + 1;
		}
		
		return result;
	}
	
	// 저장
	@PostMapping("/save")
	public AjaxResult saveCheckTarget(
			@RequestParam(value = "id", required = false) Integer id,
			@RequestParam(value = "check_master_id", required = false) Integer check_master_id,
			@RequestParam(value = "target_name", required = false) String target_name,
    		@RequestParam(value = "group1", required=false) String group1,
    		@RequestParam(value = "group2", required=false) String group2,
    		@RequestParam(value = "start_date", required=false) String start_date,
    		@RequestParam(value = "end_date", required=false) String end_date,
			HttpServletRequest request,
			Authentication auth) throws ParseException {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
        
		String today = DateUtil.getTodayString();        
        if (StringUtils.isEmpty(start_date)) {
        	start_date = today;
        }

        if (StringUtils.isEmpty(end_date)) {
        	end_date = today;
        }
        
        if (StringUtils.isEmpty(target_name) == false && id == null) {
        	
        	boolean unique_check = this.checkTargetRepository.findByTargetName(target_name).isEmpty();
        	
        	if (unique_check == false) {
				result.success = false;
				result.message="중복된 코드가 존재합니다.";
				return result;
        	}
        }
        
        CheckTarget ct = null;
        
        if (id == null) {        	
        	ct = new CheckTarget();
        } else {        	
        	ct = this.checkTargetRepository.getCheckTargetById(id);
        }

        ct.setCheckMasterId(check_master_id);
        ct.setTargetName(target_name);    
        ct.setTargetGroup1(group1);
        ct.setTargetGroup2(group2);     
        ct.setStartDate(Date.valueOf(start_date));
        ct.setEndDate(Date.valueOf(end_date));
        ct.set_audit(user);
        
        ct = this.checkTargetRepository.save(ct);
        
		return result;
	}
	
	// 삭제
	@PostMapping("/delete")
	public AjaxResult deleteCheckItem(
			@RequestParam(value = "id", required = false) Integer id) {
		
		AjaxResult result = new AjaxResult();
		this.checkTargetRepository.deleteById(id);
		return result;
	}	
}