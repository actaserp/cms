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

import mes.app.system.service.LabelCodeService;
import mes.domain.entity.LabelCode;
import mes.domain.entity.LabelCodeLang;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.LabelCodeLangRepository;
import mes.domain.repository.LabelCodeRepository;

@RestController
@RequestMapping("/api/system/labelcode")
public class LabelCodeController {

	@Autowired
	private LabelCodeService labelCodeService;
	
	@Autowired
	private LabelCodeRepository labelCodeRepository;
	
	@Autowired
	private LabelCodeLangRepository labelCodeLangRepository;
	
	// 라벨 코드 조회
	@GetMapping("/labelcode_read")
	public AjaxResult getLabelCodeList(
			@RequestParam(value="menu_code", required=false) String menu_code,
			HttpServletRequest request) {
		List <Map<String, Object>> items = this.labelCodeService.getLabelCodeList(menu_code);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	// 라벨 코드 저장
	@PostMapping("/labelcode_save")
	public AjaxResult saveTestMasterGroup(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="ModuleName") String ModuleName, 
			@RequestParam(value="TemplateKey") String TemplateKey, 
			@RequestParam(value="LabelCode") String LabelCode, 
			@RequestParam(value="Description") String Description, 
			HttpServletRequest request,
			Authentication auth	) {
		
		AjaxResult result = new AjaxResult();
		User user = (User)auth.getPrincipal();
		LabelCode lc = null;
		
		if (id==null) {
			lc = new LabelCode();
		} else {
			lc = this.labelCodeRepository.getLabelCodeById(id);
		}
		
		boolean labelCodeChk = this.labelCodeRepository.findByLabelCode(LabelCode).isEmpty();
		
		if(LabelCode.equals(lc.getLabelCode()) == false && labelCodeChk == false) {
			result.success = false;
			result.message = "이미 존재하는 코드입니다.";
			return result;
		}
		
		lc.setModuleName(ModuleName);
		lc.setTemplateKey(TemplateKey);
		lc.setLabelCode(LabelCode);
		lc.setDescription(Description);
		lc.set_audit(user);
		lc= this.labelCodeRepository.save(lc);
		
		
		
        result.data = lc;
		return result;
	}
	// 라벨 코드 상세
	@GetMapping("/labelcode_detail")
	public AjaxResult getLabelCodeDetail(
			@RequestParam("id") Integer id,
			HttpServletRequest request) {
		
		Map<String, Object> items = this.labelCodeService.getLabelCodeDetail(id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	//라벨 코드 삭제
	@PostMapping("/labelcode_delete")
	public AjaxResult labelcodeDelete(@RequestParam("id") Integer id) {
		this.labelCodeRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}
	
	//언어 설정 목록
	@GetMapping("/label_language_list")
	public AjaxResult getLabelLanguageList(
			@RequestParam(value="labelcode_id", required=false) Integer labelcode_id,
			@RequestParam(value="lang_code", required=false) String lang_code,
			HttpServletRequest request) {
		List <Map<String, Object>> items = this.labelCodeService.getLabelLanguageList(labelcode_id, lang_code);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	//언어 설정 저장
	@PostMapping("/labellang_save")
	public AjaxResult labellangSave(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="LabelCode_id") Integer LabelCodeId,
			@RequestParam(value="LangCode") String LangCode, 
			@RequestParam(value="DispText") String DispText, 
			HttpServletRequest request,
			Authentication auth
			) {
		
		 AjaxResult result = new AjaxResult();
		User user = (User)auth.getPrincipal();
		LabelCodeLang lcl = null;
		
		if (id==null) {
			lcl = new LabelCodeLang();
		} else {
			lcl = this.labelCodeLangRepository.getLabelCodeLangById(id);
		}
		
		int cnt = this.labelCodeService.getChkCode(LabelCodeId, LangCode);
		
		if(cnt > 0 && id == null){
			result.success = false;
			result.message = "이미 등록되어 있는 언어 코드입니다.";
			return result;
		}
		
		lcl.setLabelCodeId(LabelCodeId);
		lcl.setLangCode(LangCode);
		lcl.setDispText(DispText);
		lcl.set_audit(user);
		lcl = this.labelCodeLangRepository.save(lcl);
		
        result.data = lcl;
		return result;
	}
	
	// 언어 설정 상세보기
	@GetMapping("labellang_detail")
	public AjaxResult getLabellangDetail(
			@RequestParam("id") int id,
			HttpServletRequest request) {
	
		Map<String, Object> items = this.labelCodeService.getLabellangDetail(id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	// 언어 설정 삭제
	@PostMapping("/labellang_delete")
	public AjaxResult labellangDelete(@RequestParam("id") Integer id) {
		this.labelCodeLangRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}
}
