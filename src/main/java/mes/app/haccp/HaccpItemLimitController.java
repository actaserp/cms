package mes.app.haccp;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.haccp.service.HaccpItemLimitService;
import mes.domain.entity.HaccpItemLimit;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.HaccpItemLimitRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/haccp/haccp_item_limit")
public class HaccpItemLimitController {
	
	@Autowired
	private HaccpItemLimitService haccpItemLimitService;

	@Autowired
	HaccpItemLimitRepository haccpItemLimitRepository;
	
	// HACCP일지목록 조회
	@GetMapping("/read")
	public AjaxResult getHaccpItemLimitList(
			@RequestParam(value = "HaccpProcess_id", required = false) Integer hp_id,
			@RequestParam(value = "Material_id", required = false) Integer mat_id) {
        
		List<Map<String, Object>> items = this.haccpItemLimitService.getHaccpItemLimitList(hp_id, mat_id);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}
	
	// HACCP 항목 기준 조회
	@GetMapping("/haccp_item_list")
	public AjaxResult getHaccpItemLimitInputList(
			@RequestParam(value = "HaccpProcess_id", required = false) Integer hp_id,
			@RequestParam(value = "Material_id", required = false) Integer mat_id) {
		
		List<Map<String, Object>> items = this.haccpItemLimitService.getHaccpItemLimitInputList(hp_id, mat_id);
        
        AjaxResult result = new AjaxResult();
        result.data = items;
        
		return result;
	}

	// 저장
	@PostMapping("/haccp_item_limit_save")
	@Transactional
	public AjaxResult saveHaccpItemLimit(
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
        AjaxResult result = new AjaxResult();
        
		User user = (User)auth.getPrincipal();

		List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());

		for (int i=0; i < items.size(); i++) {
			
			Float LowSpec = items.get(i).get("LowSpec") != null ? ((Number) items.get(i).get("LowSpec")).floatValue() : null;			
			Float UpperSpec = items.get(i).get("UpperSpec") != null ? ((Number) items.get(i).get("UpperSpec")).floatValue() : null;
			String SpecText = items.get(i).get("SpecText") != null ? (String) items.get(i).get("SpecText") : null;
			Integer HaccpItemLimit_id = items.get(i).get("HaccpItemLimit_id") != null ? (Integer) items.get(i).get("HaccpItemLimit_id") : null;
			Integer HaccpProcess_id = items.get(i).get("HaccpProcess_id") != null ? (Integer) items.get(i).get("HaccpProcess_id") : null;
			Integer HaccpItem_id = items.get(i).get("HaccpItem_id") != null ? (Integer) items.get(i).get("HaccpItem_id") : null;
			Integer Material_id = items.get(i).get("Material_id") != null ? (Integer) items.get(i).get("Material_id") : null;
			
			HaccpItemLimit haccp_item_limit = null;
			
			if (HaccpItemLimit_id != null) {
				haccp_item_limit = this.haccpItemLimitRepository.getHaccpItemLimitById(HaccpItemLimit_id);				
			} else {
				
				boolean exists = this.haccpItemLimitRepository.findByMaterialIdAndHaccpItemIdAndHaccpProcessId(Material_id, HaccpItem_id, HaccpProcess_id).isEmpty();
				
				if (exists == false) {
                	haccp_item_limit = this.haccpItemLimitRepository.findByMaterialIdAndHaccpItemIdAndHaccpProcessId(Material_id, HaccpItem_id, HaccpProcess_id).get(0);
                } else {
                	haccp_item_limit = new HaccpItemLimit();
                	
                	haccp_item_limit.setMaterialId(Material_id);
                	haccp_item_limit.setHaccpItemId(HaccpItem_id);
                	haccp_item_limit.setHaccpProcessId(HaccpProcess_id);
                }
			}
			haccp_item_limit.setLowSpec(LowSpec);
            haccp_item_limit.setUpperSpec(UpperSpec);
            haccp_item_limit.setSpecText(SpecText);
            haccp_item_limit.set_audit(user);
			
            this.haccpItemLimitRepository.save(haccp_item_limit);
		}
		
		return result;
	}	
}
