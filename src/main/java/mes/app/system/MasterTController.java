package mes.app.system;

import java.sql.Timestamp;
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

import mes.app.system.service.MasterTService;
import mes.domain.entity.MasterT;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.MasterTRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/system/master_t")
public class MasterTController {

	@Autowired
	private MasterTService masterTService;
	
	@Autowired
	MasterTRepository masterTRepository;
	
	@GetMapping("/read")
	public AjaxResult getMasterT(
    		@RequestParam(value="master_class") String masterClass,
    		@RequestParam(value="type_class_code") String typeClassCode,
    		@RequestParam(value="type2_class_code", required=false) String type2ClassCode,
    		@RequestParam(value="type_class_table") String typeClassTable,
    		@RequestParam(value="type2_class_table", required=false) String type2ClassTable,
    		@RequestParam(value="type", required=false) String type,
    		@RequestParam(value="type2", required=false) String type2,
    		@RequestParam(value="base_date") String baseDate) {
		
        List<Map<String, Object>> items = this.masterTService.getMasterT(masterClass,typeClassCode,typeClassTable,baseDate, type2ClassCode, type2ClassTable, type, type2);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult getMasterTDetail(
			@RequestParam("id") int id) {
		Map<String, Object> items = this.masterTService.getMasterTDetail(id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@PostMapping("/save")
	public AjaxResult saveMasterT(
			@RequestParam(value="id", required = false) Integer id,
			@RequestParam(value="master_class", required = false) String masterClass,
			@RequestParam(value="Code" , required=false) String code,
			@RequestParam(value="Name", required = false) String name,
			@RequestParam(value="Type", required = false) String type,
			@RequestParam(value="Type2", required=false) String type2,
			@RequestParam(value="Char1", required=false) String char1,
			@RequestParam(value="Char2", required=false) String char2,
			@RequestParam(value="area_num", required=false) String areaNum,
			@RequestParam(value="Number1", required = false) String number1, 
			@RequestParam(value="Date1", required=false) String date1,
			@RequestParam(value="Text1", required=false) String text1,
			@RequestParam(value="Description", required = false) String description,
			@RequestParam(value="StartDate", required = false) String startDate,
			@RequestParam(value="EndDate", required = false) String endDate,
			HttpServletRequest request,
			Authentication auth
			) {
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		
		MasterT mt = null;
		
		if (id == null) {
			mt = new MasterT(); 
			mt.set_order(10000);
		} else {
			mt = this.masterTRepository.getMasterTById(id);
		}
		boolean codeCheck = this.masterTRepository.findByCodeAndMasterClass(code,masterClass).isEmpty();
		
		if (code != null) {
			if (code.equals(mt.getCode()) == false && codeCheck == false) {
				result.message = "이미 존재하는 코드입니다.";
				result.success = false;
				return result;
			}			
		}
		
		Timestamp start = CommonUtil.tryTimestamp(startDate);
		Timestamp end = CommonUtil.tryTimestamp(endDate);
		
		if (char1 == null) {
			char1 = areaNum;
		}
		
		mt.setMasterClass(masterClass != null ? masterClass : "");
		mt.setCode(code != null ? code : "");
		mt.setName(name != null ? name : "");
		mt.setType(type != null ? type : "");
		mt.setType2(type2 != null ? type2 : "");
		mt.setChar1(char1 != null ? char1 : "");
		mt.setChar2(char2 != null ? char2 : "");
		mt.setNumber1(number1 != null ? Float.parseFloat(number1) : null);
		mt.setDate1(date1 != null ? Timestamp.valueOf(date1) : null);
		mt.setText1(text1 != null ? text1 : "");
		mt.setDescription(description != null ? description : "");
		mt.setStartDate(start);
		mt.setEndDate(end);
		mt.set_audit(user);
        
		mt = this.masterTRepository.save(mt);
		
		result.data = mt;
		return result;
		
	}
	
	@PostMapping("/code_order_save")
	@Transactional
	public AjaxResult codeOrderSave(
			@RequestParam(value="Q[]") List<String> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();

		Integer order = 1;
		 
		for(int i = 0; i < Q.size(); i++) {
			Integer id = Integer.parseInt(Q.get(i));
			MasterT mt = this.masterTRepository.getMasterTById(id);
			mt.set_order(order);
			mt.set_audit(user);
			order += 1;
			mt = this.masterTRepository.save(mt);
			result.data = mt;
		}
		
		return result;
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteMasterT(@RequestParam("id") Integer id) {
		this.masterTRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}
}
