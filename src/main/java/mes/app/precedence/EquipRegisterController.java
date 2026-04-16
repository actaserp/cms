package mes.app.precedence;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.precedence.service.EquipRegisterService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/precedence/equip_register")
public class EquipRegisterController {
	
	@Autowired
	private EquipRegisterService equipRegisterService;
		
	@GetMapping("/read")
	public AjaxResult getEquipRegisterList(
    		@RequestParam(value="area_id", required=false) Integer area_id,
			HttpServletRequest request) {

        List<Map<String, Object>> items = this.equipRegisterService.getEquipRegisterList(area_id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult getEquipRegisterDetailList(
    		@RequestParam(value="id", required=false) Integer id,
			HttpServletRequest request) {

		Map<String, Object> items = this.equipRegisterService.getEquipRegisterDetailList(id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
			
	
}
