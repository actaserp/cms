package mes.app.definition;

import java.util.Arrays;
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


import mes.domain.model.AjaxResult;
import mes.app.definition.service.WorkcenterService;
import mes.domain.entity.RelationData;
import mes.domain.entity.User;
import mes.domain.entity.Workcenter;
import mes.domain.repository.RelationDataRepository;
import mes.domain.repository.WorkcenterRepository;

@RestController
@RequestMapping("/api/definition/workcenter")
public class WorkcenterController {
	
	@Autowired
	WorkcenterRepository workcenterRepository;
	
	@Autowired
	RelationDataRepository relationRepository;
	
	@Autowired
	private WorkcenterService workcenterService;
	
	// 워크센터 목록 조회
	@GetMapping("/read")
	public AjaxResult getWorkcenterList(
			@RequestParam(value="keyword", required=false) String keyword,
			@RequestParam(value ="spjangcd") String spjangcd,
			HttpServletRequest request) {
		
		List<Map<String, Object>> items = this.workcenterService.getWorkcenterList(keyword,spjangcd);
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}
	
	// 워크센터 상세정보, 설비정보 조회
	@GetMapping("/detail")
	public AjaxResult getWorkcenterDetail(
			@RequestParam("id") int workcenterId, 
    		HttpServletRequest request) {
        Map<String, Object> item1 = this.workcenterService.getWorkcenterDetail(workcenterId);
        List<Map<String, Object>> item2 = this.workcenterService.getEquipmentList(workcenterId);
               		
        List<Object> items = Arrays.asList(item1, item2);
        
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	// 설비정보 조회
	@GetMapping("/equipment_list")
	public AjaxResult getEquipmentList(
			@RequestParam("id") int workcenterId, 
			HttpServletRequest request) {
		
		List<Map<String, Object>> items = this.workcenterService.getEquipmentList(workcenterId);
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}
	
	// 워크센터 저장
	@PostMapping("/save")
	public AjaxResult saveWorkcenter(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="factory_id") Integer factoryId,
			@RequestParam(value="area_id", required=false) Integer areaId,
			@RequestParam(value="Process_id") Integer processId,
			@RequestParam(value="code") String code,
			@RequestParam(value="name") String name,
			@RequestParam(value="process_storehouse_id", required=false) Integer processStorehouseId,
			@RequestParam(value="outsourcing_yn", required=false) String outsourcingYN,
			@RequestParam(value ="spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
        AjaxResult result = new AjaxResult();
        
		Workcenter workcenter =null;
		
		if (id==null) {
			workcenter = new Workcenter();
		} else {
			workcenter = this.workcenterRepository.getWorkcenterById(id);
		}
		
		boolean codeChk = this.workcenterRepository.findByCode(code).isEmpty();

		if (code.equals(workcenter.getCode()) == false && codeChk == false) {
			result.success = false;
			result.message="중복된 워크센터코드 입니다.";
			return result;
		}
		
		boolean nameChk = this.workcenterRepository.findByName(name).isEmpty();

		if (name.equals(workcenter.getName()) == false && nameChk == false) {
			result.success = false;
			result.message="중복된 워크센터명 입니다.";
			return result;
		}
		workcenter.setCode(code);
		workcenter.setName(name);
		workcenter.setFactoryId(factoryId);
		workcenter.setAreaId(areaId);
		workcenter.setProcessId(factoryId);
		workcenter.setProccesStoreHouseId(processStorehouseId);
		workcenter.setOutSourcingYN(outsourcingYN);
		workcenter.setProcessId(processId);
		workcenter.set_audit(user);
		workcenter.setSpjangcd(spjangcd);

		
		workcenter = this.workcenterRepository.save(workcenter);
		
        result.data=workcenter;
		
		return result;
	}
	
	// 설비정보 저장
		@PostMapping("/add_equipment")
		public AjaxResult addEquipment(
				@RequestParam("workcenter_id") Integer workcenter_id,
				@RequestParam("equipment_id") Integer equipment_id,
				HttpServletRequest request,
				Authentication auth) {
			
			User user = (User)auth.getPrincipal();
			AjaxResult result1 = new AjaxResult();
			
			boolean isSameData = this.workcenterService.checkEquipment(workcenter_id, equipment_id);
			
			if (isSameData==true) {
				result1.success = false;
				result1.message="이미 저장된 설비정보입니다.";
				return result1;
			}
			
			RelationData reladata = null;
			reladata =  new RelationData();
			
			reladata.setDataPk1(workcenter_id);
			reladata.setTableName1("work_center");
			reladata.setDataPk2(equipment_id);
			reladata.setTableName2("equ");
			reladata.setRelationName("workcenter-equipment");
			reladata.set_audit(user);
			
			reladata = this.relationRepository.save(reladata);
			
			AjaxResult result = new AjaxResult();
	        result.data=reladata;
			
			return result;
		}
		
		// 워크센터 정보 삭제
		@PostMapping("/delete")
		public AjaxResult deleteWorkcenter(@RequestParam("id") int id) {
	        
	        this.workcenterRepository.deleteById(id);
	        AjaxResult result = new AjaxResult();
			return result;
		}
		
		// 설비정보 삭제
		@PostMapping("/delete_equipment")
		public AjaxResult deleteEquipment(
				@RequestParam("id") int id,
				@RequestParam("workcenter_id") int workcenter_id,
				@RequestParam("equipment_id") int equipment_id
				) {
			
	        this.relationRepository.deleteById(id);
	        AjaxResult result = new AjaxResult();
			return result;
		}

}
