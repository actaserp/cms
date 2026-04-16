package mes.app.system;

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

import mes.app.system.service.PropertyMasterService;
import mes.domain.entity.PropertyMaster;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.PropertyMasterRepository;

@RestController
@RequestMapping("/api/system/property_master")
public class PropertyMasterController {
	
	@Autowired
	private PropertyMasterService propertyMasterService;

	@Autowired
	private PropertyMasterRepository propertyMasterRepository;

	// 프로퍼티 리스트 조회
	@GetMapping("/read")
	public AjaxResult getPropertyMasterList(
			@RequestParam(value="tb_name", required=false) String tb_name,
			HttpServletRequest request) {

		List<Map<String, Object>> items = this.propertyMasterService.getPropertyMasterList(tb_name);
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}
	
	// 프로퍼티 상세 조회
	@GetMapping("/detail")
	public AjaxResult getPropertyMasterDetail(
			@RequestParam("id") int id, 
			HttpServletRequest request) {
        Map<String, Object> items = this.propertyMasterService.getPropertyMasterDetail(id);
        
        AjaxResult result = new AjaxResult();
        result.data = items;
        
		return result;
	}
	
	// 프로퍼티 저장
	@PostMapping("/save")
	public AjaxResult savePropertyMaster(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="table_name", required=false) String table_name,
			@RequestParam(value="table_code", required=false) String table_code,
			@RequestParam(value="description", required=false) String description,
			@RequestParam(value="val_type", required=false) String val_type,
			@RequestParam(value="new_data", required=false) String new_data,
			HttpServletRequest request,
			Authentication auth) {

        AjaxResult result = new AjaxResult();
		User user = (User)auth.getPrincipal();
		
		PropertyMaster propertyMaster = null;
		
		boolean chkNameCode = this.propertyMasterRepository.findByTableNameAndCode(table_name, table_code).isEmpty();
		
		try {
			if (id != null) {
				if ("Y".equals(new_data) && chkNameCode == false) {	// alert
					result.success = false;
					result.message = "이미 존재하는 코드입니다.";
					return result;
				} else if ("Y".equals(new_data)) {	// insert
					propertyMaster = new PropertyMaster();
				} else {	// update
					propertyMaster = this.propertyMasterRepository.getPropMasterById(id);
					propertyMaster.setId(id);
				}
			} else {	// new
				if (chkNameCode == false) {		// alert
					result.success = false;
					result.message = "이미 존재하는 코드입니다.";
					return result;
				} else {	// insert
					propertyMaster = new PropertyMaster();
				}
			}

			propertyMaster.setTableName(table_name);
	        propertyMaster.setCode(table_code);
			propertyMaster.setType(val_type);
			propertyMaster.setDescription(description);
			propertyMaster.set_audit(user);
			
			propertyMaster = this.propertyMasterRepository.save(propertyMaster);
			
			result.data = propertyMaster;
		} catch (Exception e) {
			result.success = false;
			result.message = e.toString();
		}
		
		return result;
	}
	
	// 프로퍼티 삭제 
	@PostMapping("/delete")
	public AjaxResult deletePropertyMaster(@RequestParam("id") Integer id) {
		this.propertyMasterRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}
}
