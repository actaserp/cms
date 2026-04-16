package mes.app.system;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.system.service.DasConfigService;
import mes.domain.entity.DasConfig;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.DasConfigRepository;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/system/das_config")
public class DasConfigController {
	
	@Autowired
	private DasConfigService dasConfigService;
		
	@Autowired
	DasConfigRepository dasConfigRepository;
	
	@Autowired
	SqlRunner sqlRunner;

	
	// searchMainData
	@GetMapping("/read")
	public AjaxResult getDasConfigList(
    		@RequestParam(value="server", required=false) Integer server_id, 
    		@RequestParam(value="equipment", required=false) Integer equipment_id, 
			HttpServletRequest request) {
		
		List<Map<String, Object>> items = this.dasConfigService.getDasConfigList(server_id, equipment_id);      
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}
	
	// showDetail
	@GetMapping("/detail")
	public AjaxResult getDasConfigDetail(
			@RequestParam(value="id") Integer id,
			HttpServletRequest request) {
		
		Map<String, Object> item = this.dasConfigService.getDasConfigDetail(id); 
		AjaxResult result = new AjaxResult();
		result.data = item;
		return result;
	}
		
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveDasConfigData(
			@RequestParam(value="id", required = false) Integer id,
			@RequestParam(value="is_active", required = false, defaultValue = "N") String is_active,
			@RequestParam(value="Server_id", required = false) Integer Server_id,
			@RequestParam(value="Equipment_id", required = false) Integer Equipment_id,
			@RequestParam(value="Name", required = false) String Name,
			@RequestParam(value="Description", required = false) String Description,
			@RequestParam(value="Configuration", required = false) String Configuration,
			@RequestParam(value="ConfigFileName", required = false) String ConfigFileName,
			@RequestParam(value="Handler", required = false) String Handler,
			@RequestParam(value="DeviceType", required = false) String DeviceType,
			@RequestParam(value="Topic", required = false) String Topic,
			HttpServletRequest request,
			Authentication auth
			) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		DasConfig dc = null;
		
		if(id != null) {
			dc = this.dasConfigRepository.getDasConfigById(id);
		}else {
			dc = new DasConfig();
		}
		
		dc.setServer_id(Server_id);
		dc.setEquipment_id(Equipment_id);
		dc.setName(Name);
		dc.setDescription(Description);
		dc.setConfiguration(Configuration);
		dc.setConfigFileName(ConfigFileName);
		dc.setHandler(Handler);
		dc.setDeviceType(DeviceType);
		dc.setTopic(Topic);
		dc.setIs_active(is_active);
		dc.set_audit(user);
		dc = this.dasConfigRepository.save(dc);
				
//		Map<String, Object> item = new HashMap<String, Object>();
//		item.put("id", dc.getId());
//		result.data = item;
		result.success=true;
		return result;
	}
	
	// deleteData
	@PostMapping("/delete")
	public AjaxResult deleteDasConfigData(@RequestParam("id") int id) {
		this.dasConfigRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		
		result.success=true;
		return result;
	}
}
