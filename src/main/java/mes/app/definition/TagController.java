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

import mes.app.definition.service.TagService;
import mes.domain.entity.Tag;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.TagRepository;


@RestController       
@RequestMapping("/api/definition/tag")
public class TagController {
	
	@Autowired
	TagService tagService;
	
	@Autowired
	TagRepository tagRepository;
	
	
	
	
	
	@GetMapping("/read")
	private AjaxResult getTagResult(
			@RequestParam("keyword") String keyword,
			@RequestParam("tag_group_id") String tag_group_id,
			@RequestParam("equipment_id") String equipment_id) {
		
		List<Map<String, Object>> items = this.tagService.getTagResult(keyword, tag_group_id,equipment_id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/detail")
	private AjaxResult getEduResultDetail(
			@RequestParam(value = "tag_code", required = false) String tag_code) {
		
		Map<String, Object> items = this.tagService.getTagpResultDetail(tag_code);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@PostMapping("/save")
	private AjaxResult saveTagResult(
			@RequestParam(value = "id", required = false) Integer id,
			@RequestParam(value = "tag_code", required = false) String tag_code,
			@RequestParam(value = "tag_name", required = false) String tag_name,
			@RequestParam(value = "tag_group_id", required = false) Integer tag_group_id,
			@RequestParam(value = "equipment_id", required = false) Integer equipment_id,
			@RequestParam(value = "round_digit", required = false) Integer round_digit,
			@RequestParam(value = "DASConfig_id", required = false) Integer DASConfig_id,
			@RequestParam(value = "lsl", required = false) Float lsl,
			@RequestParam(value = "usl", required = false) Float usl,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		Tag t = null;
		
		
		if ( id == null) {
			t = new Tag();
			t.setTagCode(tag_code);
		} else {
			t = this.tagRepository.getByTagCode(tag_code);
		}
		t.setTag_name(tag_name);
		t.setTag_group_id(tag_group_id);
		t.setEquipment_id(equipment_id);
		t.setRound_digit(round_digit);
		t.setDASConfig_id(DASConfig_id);
		t.setLSL(lsl);
		t.setUSL(usl);
		t.set_audit(user);
		
		t = this.tagRepository.save(t);
		
		
		result.data = t;
		
		return result;
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteTagResult(
			@RequestParam(value = "tag_code", required = false) String tag_code) {
		
		AjaxResult result = new AjaxResult();
		
		this.tagService.deleteByTagCode(tag_code);
		
		return result;
	}
	
}
