package mes.app.haccp;

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

import mes.app.haccp.service.PestControlStandardService;
import mes.domain.entity.PestControlStandard;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.PestControlStandardRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/haccp/pest_control_standard")
public class PestControlStandardController {

	@Autowired
	private PestControlStandardService pestControlStandardService;
	
	@Autowired
	PestControlStandardRepository pestControlStandardRepository;
	
	@GetMapping("/read")
	public AjaxResult getPestControlList(
			@RequestParam(value="haccp_area_class_code") String haccpAreaClassCode,
			@RequestParam(value="pest_class_code") String pestClassCode,
			@RequestParam(value="season_code") String seasonCode) {
		
        List<Map<String, Object>> items = this.pestControlStandardService.getPestControlList(haccpAreaClassCode,pestClassCode,seasonCode);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult getPestControlDetail(
			@RequestParam("id") int id) {
		
		Map<String, Object> items = this.pestControlStandardService.getPestControlDetail(id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@PostMapping("/save")
	public AjaxResult savePestControl(
			@RequestParam(value = "id", required = false) Integer id,
			@RequestParam(value= "ActionContent", required = false) String actionContent,
			@RequestParam(value = "FromCount", required = false) String fromCount,
			@RequestParam(value= "HaccpAreaClassCode", required = false) String HaccpAreaClassCode,
			@RequestParam(value = "PestClassCode", required = false) String pestClassCode,
			@RequestParam(value= "SeasonCode", required = false) String seasonCode,
			@RequestParam(value= "ToCount", required = false) String toCount,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		PestControlStandard pcs = null;
		if (id != null) {
			pcs = this.pestControlStandardRepository.getPestControlStandardById(id);
		} else {
			pcs = new PestControlStandard();
		}
		
		pcs.setHaccpAreaClassCode(HaccpAreaClassCode);
		pcs.setPestClassCode(pestClassCode);
		pcs.setSeasonCode(seasonCode);
		pcs.setFromCount(CommonUtil.tryIntNull(fromCount));
		pcs.setToCount(CommonUtil.tryIntNull(toCount));
		pcs.setActionContent(actionContent);
		pcs.set_audit(user);
		
		pcs = this.pestControlStandardRepository.save(pcs);
		
		result.data = pcs;
		return result;
	}
	
	@PostMapping("/delete")
	public AjaxResult deletePestControl(@RequestParam("id") Integer id) {
		this.pestControlStandardRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}
	
	
}
