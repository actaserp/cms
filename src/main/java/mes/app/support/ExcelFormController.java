package mes.app.support;

import java.sql.Date;
import java.util.HashMap;
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

import mes.app.support.service.ExcelFormService;
import mes.domain.entity.DocForm;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.DocFormRepository;

@RestController
@RequestMapping("/api/support/excel_form")
public class ExcelFormController {
	
	@Autowired
	private ExcelFormService excelFormService;
	
	@Autowired
	DocFormRepository docFormRepository;
	
	// searchMainData
	@GetMapping("/read")
	public AjaxResult getExcelFormList(
			@RequestParam("keyword") String keyword) {
		
		List<Map<String, Object>> items = this.excelFormService.getExcelFormList(keyword);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	// 엑셀양식 상세정보 조회
	@GetMapping("/detail")
	public AjaxResult getExcelFormDetail(
			@RequestParam("id") Integer id) {
		
		Map<String, Object> items = this.excelFormService.getExcelFormDetail(id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	// 엑셀양식 저장
	@Transactional
	@PostMapping("/save")
	public AjaxResult saveExcelForm(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="form_name", required=false) String form_name,
			@RequestParam(value="start_date", required=false) String start_date,
			@RequestParam(value="end_date", required=false) String end_date,
			@RequestParam(value = "form_value", required = false) String form_value,
			@RequestParam(value="description", required=false) String description,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		DocForm df = null;
		
		if(id != null) {
			df = this.docFormRepository.getDocFormById(id);
		}else {
			df = new DocForm();
			df.setFormType("excel");
		}
		
		df.setFormName(form_name);
		df.setStartDate(Date.valueOf(start_date));
		df.setEndDate(Date.valueOf(end_date));
		df.setContent(form_value);
		df.setDescription(description);
		df.setFormGroup("");
		df.set_audit(user);
		df = this.docFormRepository.save(df);
		Integer form_id = df.getId();
				
		Map<String, Object> item = new HashMap<String, Object>();
		item.put("id", form_id);
		result.data = item;
		return result;
	}
	
	// 엑셀양식 삭제
	@PostMapping("/delete")
	public AjaxResult deleteExcelForm(
			@RequestParam(value="id", required=false) Integer id) {
		
		AjaxResult result = new AjaxResult();
		
		if(id != null) {
			this.docFormRepository.deleteById(id);
			result.success = true;
		}else {
			result.success = false;
		}
		
		return result;
	}
}
