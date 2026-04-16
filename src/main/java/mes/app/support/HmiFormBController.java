package mes.app.support;

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

import mes.app.support.service.HmiFormBService;
import mes.domain.entity.DocForm;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.DocFormRepository;

@RestController
@RequestMapping("/api/support/hmi_b")
public class HmiFormBController {

	@Autowired
	private HmiFormBService hmiFormBService;

	@Autowired
	DocFormRepository docFormRepository;
	// HMI양식B 리스트 조회
	@GetMapping("/read")
	public AjaxResult getHmiBList(@RequestParam(value="keyword", required=false) String keyword,
			HttpServletRequest request) {

		List<Map<String, Object>> items = this.hmiFormBService.getHmiBList(keyword);
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}

	// HMI양식B 리스트 상세조회
	@GetMapping("/detail")
	public AjaxResult getHmiBDetail(@RequestParam(value="id", required=false) String form_id, HttpServletRequest request) {
        Map<String, Object> items = this.hmiFormBService.getHmiBDetail(Integer.parseInt(form_id));
        
        AjaxResult result = new AjaxResult();
        result.data = items;
        result.success = true;
        
		return result;
	}
	
	@GetMapping("/image_list")
	public AjaxResult getImageList() {
		
		List<Map<String, Object>> items = this.hmiFormBService.getImageList();
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
		
	}
	
	@GetMapping("/background_image_list")
	public AjaxResult getBackgroundImageList() {
		
		List<Map<String, Object>> items = this.hmiFormBService.getBackgroundImageList();
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
		
	}
	
	@PostMapping("/save")
	public AjaxResult saveHmiFormB(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="form_name", required=false) String formName,
			@RequestParam(value="content", required=false) String content,
			@RequestParam(value="description", required=false) String description,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		User user = (User)auth.getPrincipal();
		
		DocForm df = null;
		
		if (id != null) {
			df = this.docFormRepository.getDocFormById(id);
		} else {
			df = new DocForm();
			df.setFormGroup("");
		}
		
		df.setFormType("hmi_b");
		df.setFormName(formName);
		df.setContent(content);
		df.setDescription(description);
		df.set_audit(user);
		
		df = this.docFormRepository.save(df);
		
		result.data = df;
		result.success = true;
		
		return result;
		
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteHmiFormB(
			@RequestParam(value="id", required=false) Integer id) {
		
		AjaxResult result = new AjaxResult();
		
		this.docFormRepository.deleteById(id);
		
		return result;
	}
		
}
