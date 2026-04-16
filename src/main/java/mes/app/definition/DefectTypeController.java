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

import mes.app.definition.service.DefectTypeService;
import mes.domain.entity.DefectType;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.DefectTypeRepository;

@RestController
@RequestMapping("/api/definition/defect_type")
public class DefectTypeController {

	@Autowired
	DefectTypeRepository DefectTypeRepository;
	
	@Autowired
	private DefectTypeService DefectTypeService;
	
	// 부적합 유형 목록 조회 
	@GetMapping("/read")
	public AjaxResult getDefectTypeList(
			@RequestParam("keyword") String keyword,
			HttpServletRequest request) {
		
		List<Map<String, Object>> items = this.DefectTypeService.getDefectTypeList(keyword);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	// 부적합 유형 상세정보 조회 
	@GetMapping("/detail")
	public AjaxResult getDefectTypeDetail(
			@RequestParam("id") int id,
			HttpServletRequest request) {
		Map<String, Object> item = this.DefectTypeService.getDefectTypeDetail(id);
		
		AjaxResult result = new AjaxResult();
		result.data = item;
		
		return result;
	}
	
	// 부적합 유형 저장
	@PostMapping("/save")
	public AjaxResult saveDefectType(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="defect_type_code") String defectTypeCode,
			@RequestParam(value="defect_type_name") String defectTypeName,
			@RequestParam(value="description") String description,
			HttpServletRequest request,
			Authentication auth ) {
		User user = (User)auth.getPrincipal();
		DefectType defecttype = null;
		AjaxResult result = new AjaxResult();
		
		if (id == null) {
			defecttype = new DefectType();
		} else {
			defecttype = this.DefectTypeRepository.getDefectTypeById(id);
		}
		boolean check_name = this.DefectTypeRepository.findByName(defectTypeName).isEmpty();
		boolean check_code = this.DefectTypeRepository.findByCode(defectTypeCode).isEmpty();
		
		if (!check_name && id == null) {
			result.success = false;
			result.message="중복된 부적합유형명이 존재합니다.";
			return result;
		}
		if (!check_code && id == null) {
			result.success = false;
			result.message="중복된 코드가 존재합니다.";
			return result;
		}
		
		defecttype.setCode(defectTypeCode);
		defecttype.setName(defectTypeName);
		defecttype.setDescription(description);
		defecttype.setCoverage("all");
		defecttype.set_audit(user);
		
		defecttype = this.DefectTypeRepository.save(defecttype);
		
		result.data = defecttype;
		return result;
	}
	
	// 부적합 유형 삭제 
	@PostMapping("/delete")
	public AjaxResult deleteDefectType(@RequestParam("id") Integer id) {
	
		this.DefectTypeRepository.deleteById(id);
		
		AjaxResult result = new AjaxResult();
		
		return result;
	}
	
}
