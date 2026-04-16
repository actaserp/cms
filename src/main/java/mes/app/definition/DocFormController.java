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

import mes.app.definition.service.DocFormSevice;
import mes.domain.entity.DocForm;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.DocFormRepository;

@RestController
@RequestMapping("/api/definition/doc_form")
public class DocFormController {

	@Autowired
	private DocFormSevice docFormSevice;

	@Autowired
	DocFormRepository docFormRepository;
	
	// 문서종류 리스트 조회
	@GetMapping("/read")
	public AjaxResult getDocFormList(@RequestParam(value="keyword", required=false) String keyword,
			HttpServletRequest request) {

		List<Map<String, Object>> items = this.docFormSevice.getDocFormList("file", keyword);
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}
		
	// 문서종류 상세 조회
	@GetMapping("/detail")
	public AjaxResult getDocFormDetail(@RequestParam("id") int workcenterId, HttpServletRequest request) {
        Map<String, Object> items = this.docFormSevice.getDocFormDetail(workcenterId);
        
        AjaxResult result = new AjaxResult();
        result.data = items;
        
		return result;
	}
	
	// 문서종류 저장
	@PostMapping("/save")
	public AjaxResult saveDocForm(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="form_group", required=false) String formGroup,
			@RequestParam(value="form_name", required=false) String formName,
			@RequestParam(value="description", required=false) String description,
			HttpServletRequest request,
			Authentication auth) {

        AjaxResult result = new AjaxResult();
		User user = (User)auth.getPrincipal();
		
		DocForm docForm = null;
		
		if (id == null) {
			docForm = new DocForm();
		} else {
			docForm = this.docFormRepository.getDocFormById(id);
		}
		
		boolean nameChk = this.docFormRepository.findByFormName(formName).isEmpty();

		if (formName.equals(docForm.getFormName()) == false && nameChk == false) {
			result.success = false;
			result.message="중복된 문서종류가 존재합니다.";
			return result;
		}
		
		if (id == null) {
			docForm.setFormType("file");
			docForm.setState("confirmed");			
		} else {
			docForm.setId(id);
		}
		docForm.setFormGroup(formGroup);
		docForm.setFormName(formName);
		docForm.setDescription(description);
		docForm.set_audit(user);
		
		docForm = this.docFormRepository.save(docForm);
		
        result.data = docForm;
		
		return result;
	}

	// 문서종류 삭제
	@PostMapping("/delete")
	public AjaxResult deleteDocForm(@RequestParam("id") Integer id) {
		this.docFormRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}

}
