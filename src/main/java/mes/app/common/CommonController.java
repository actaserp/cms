package mes.app.common;

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

import mes.app.system.service.SystemService;
import mes.domain.entity.LabelCode;
import mes.domain.entity.LabelCodeLang;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.LabelCodeLangRepository;
import mes.domain.repository.LabelCodeRepository;
import mes.domain.services.ComboService;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/common")
public class CommonController {

	@Autowired
	SystemService systemService;
	
	@Autowired
	ComboService comboService;
	
	@Autowired
	LabelCodeRepository labelCodeRepository;
	
	@Autowired
	LabelCodeLangRepository labelCodeLangRepository;
	
	@Autowired
	SqlRunner sqlRunner;

	@GetMapping("/labels")
	public AjaxResult labes(
			@RequestParam(value="lang_code", defaultValue="ko",  required = false) String lang_code,  
			@RequestParam(value="gui_code", required = false) String gui_code,  
			@RequestParam(value="template_key", required = false) String template_key
			) {		
        List<Map<String, Object>> items = this.systemService.getLabelList(lang_code, gui_code, template_key);               		
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/labels/labelcodelang_detail")
	public AjaxResult getLabelCodeLangDetail(
			@RequestParam(value="gui_code", required = true) String guiCode,
			@RequestParam(value="label_code", required = true) String labelCode,
			@RequestParam(value="lang_code", required = true) String langCode,
			@RequestParam(value="template_key", required = true) String templateKey) {
		
		Map<String, Object> items = this.systemService.getLabelCodeLangDetail(guiCode,labelCode,langCode,templateKey);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@PostMapping("/labels/save_labelcode_lang")
	@Transactional
	public AjaxResult saveLabelCodeLang(
			@RequestParam(value="lable_code_id", required = true) String lableCodeId,
			@RequestParam(value="ModuleName", required = true) String guiCode,
			@RequestParam(value="TemplateKey", required = true) String templateKey,
			@RequestParam(value="LabelCode", required = true) String labelCode,
			@RequestParam(value="Description", required = true) String description,
			@RequestParam(value="LangCode", required = true) String langCode,
			@RequestParam(value="DispText", required = true) String dispText,
			HttpServletRequest request,
			Authentication auth) {
		
			User user = (User)auth.getPrincipal();
			
			AjaxResult result = new AjaxResult();
			
			LabelCode lc = null;
			if (lableCodeId.isEmpty()) {
				lc = new LabelCode();
			} else {
				lc = this.labelCodeRepository.getLabelCodeById(Integer.parseInt(lableCodeId));
			}
			lc.setModuleName(guiCode);
			lc.setTemplateKey(templateKey);
			lc.setLabelCode(labelCode);
			lc.setDescription(description);
			lc.set_audit(user);
			lc = this.labelCodeRepository.save(lc);
			
			
			this.labelCodeRepository.flush();
			
			LabelCodeLang lcl = null;
			
			List<LabelCodeLang> lcList = this.labelCodeLangRepository.findByLangCodeAndLabelCodeId(langCode,lc.getId());
			
			if (lcList.size() > 0) {
				lcl = lcList.get(0);
			} else {
				lcl = new LabelCodeLang();
			}
			
			lcl.setLabelCodeId(lc.getId());
			lcl.setLangCode(langCode);
			lcl.setDispText(dispText);
			lcl.set_audit(user);
			
			lcl = this.labelCodeLangRepository.save(lcl);
			
			result.data = lcl.getId();
			
			return result;
			
		
		}
	
	//@Cacheable(value="combo",key="{#comboType, #cond1, #cond2, cond3}")	
	@GetMapping("/combo")
	public AjaxResult combo(
			@RequestParam(value="combo_type", required = true) String comboType,  
			@RequestParam(value="cond1", required = false) String cond1,  
			@RequestParam(value="cond2", required = false) String cond2,
			@RequestParam(value="cond3", required = false) String cond3
			) {		
        List<Map<String, Object>> items = this.comboService.getComboList(comboType, cond1, cond2, cond3);
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}
}
