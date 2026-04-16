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

import mes.domain.repository.MaterialGroupRepository;
import mes.app.definition.service.material.MaterialGroupService;
import mes.domain.entity.MaterialGroup;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/definition/material_group")
public class MaterialGroupController {
	
	@Autowired
	MaterialGroupRepository materialGroupRepository;
	
	@Autowired
	private MaterialGroupService materialGroupService;
	
	// 품목그룹 목록 조회
	@GetMapping("/read")
	public AjaxResult getMatGrouptList(
			@RequestParam("mat_type") String matType,
			@RequestParam("mat_grp") String matGrp,
			@RequestParam(value ="spjangcd") String spjangcd,
    		HttpServletRequest request) {
       
        List<Map<String, Object>> items = this.materialGroupService.getMatGrouptList(matType, matGrp,spjangcd);
               		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	// 품목그룹 상세정보 조회
	@GetMapping("/detail")
	public AjaxResult getMatGroupDetail(
			@RequestParam("id") int matGrpId, 
    		HttpServletRequest request) {
        Map<String, Object> item = this.materialGroupService.getMatGroupDetail(matGrpId);      
               		
        AjaxResult result = new AjaxResult();
        result.data = item;        				
        
		return result;
	}
	
	// 품목그롭 저장
	@PostMapping("/save")
	public AjaxResult saveMatGroup(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam("material_type") String matType,
			@RequestParam("material_group_code") String code,
			@RequestParam("material_group_name") String name,
			@RequestParam(value ="spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		MaterialGroup matGrp = null;
		AjaxResult result = new AjaxResult();
		
		boolean code_chk = this.materialGroupRepository.findByCode(code).isEmpty();
		
		boolean name_chk = this.materialGroupRepository.findByName(name).isEmpty();
		
		if(id == null) {
			matGrp = new MaterialGroup();
		} else {
			matGrp = this.materialGroupRepository.getMatGrpById(id);
		}
		
		if (code.equals(matGrp.getCode())==false && code_chk == false) {
			result.success = false;
			result.message="중복된 설비그룹코드가 존재합니다.";
			return result;
		}
		
		if (name.equals(matGrp.getName())==false && name_chk == false) {
			result.success = false;
			result.message="중복된 설비그룹명이 존재합니다.";
			return result;
		}
		
		matGrp.setName(name);
		matGrp.setCode(code);
		matGrp.setMaterialType(matType);
		matGrp.set_audit(user);
		matGrp.setSpjangcd(spjangcd);

		
		matGrp = this.materialGroupRepository.save(matGrp);
		
        result.data=matGrp;
		return result;
	}
	
	// 품목그룹 삭제
	@PostMapping("/delete")
	public AjaxResult deleteMatGroup(@RequestParam("id") Integer id) {
		this.materialGroupRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}

}
