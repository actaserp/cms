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

import mes.app.definition.service.EquipmentGroupService;
import mes.domain.entity.EquipmentGroup;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.EquipmentGroupRepository;

@RestController
@RequestMapping("/api/definition/equipment_group")
public class EquipmentGroupController {
	
	@Autowired
	private EquipmentGroupService equipGroupService;
	
	@Autowired
	private EquipmentGroupRepository equipGroupRepository;
	
	@GetMapping("/read")
	public AjaxResult getEquipGroupList(
			@RequestParam(value="keyword", required=false) String keyword,
			HttpServletRequest request) {
		
		List<Map<String, Object>> items = this.equipGroupService.getEquipGroupList(keyword);      
   		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult getEquipGroupDetail(
			@RequestParam("id") int id,
			HttpServletRequest request) {
		
		Map<String, Object> item = this.equipGroupService.getEquipGroupDetail(id);
		
        AjaxResult result = new AjaxResult();
        result.data = item;        				
        
		return result;
	}
	
	@PostMapping("/save")
	public AjaxResult saveEquipGroup(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam("equipment_group_code") String Code,
			@RequestParam("equipment_group_name") String Name,
			@RequestParam("equipment_type") String Type,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		AjaxResult result = new AjaxResult();
		EquipmentGroup equipGrp = null;
		
		boolean code_chk = this.equipGroupRepository.findByCode(Code).isEmpty();
		boolean name_chk = this.equipGroupRepository.findByName(Name).isEmpty();
		
		
		if (id==null) {
			equipGrp = new EquipmentGroup();
			
		} else {
			equipGrp = this.equipGroupRepository.getEquipGroupById(id);
		}
		
		if (Code.equals(equipGrp.getCode())==false && code_chk == false) {
			result.success = false;
			result.message="중복된 설비그룹코드가 존재합니다.";
			return result;
		}
		if (Name.equals(equipGrp.getName())==false && name_chk == false) {
			result.success = false;
			result.message="중복된 설비그룹명이 존재합니다.";
			return result;
		}
		
		equipGrp.setCode(Code);
		equipGrp.setName(Name);
		equipGrp.setEquipmentType(Type);
		equipGrp.set_audit(user);
		
		equipGrp = this.equipGroupRepository.save(equipGrp);
		
		result.data = equipGrp;
		return result;
			
	}
	
	
	@PostMapping("/delete")
	public AjaxResult deleteEquipGroup(@RequestParam("id") int id) {
		this.equipGroupRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		
		return result;
	}

}
