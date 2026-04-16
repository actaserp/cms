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

import mes.app.definition.service.TagGroupService;
import mes.domain.entity.TagGroup;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.TagGroupRepository;

@RestController
@RequestMapping("/api/definition/tag_group")
public class TagGroupController {
	
	@Autowired
	TagGroupService tagGroupService;
	
	@Autowired
	TagGroupRepository tagGroupRepository;
	
	
	
	
	@GetMapping("/read")
	private AjaxResult getTagGroupResult(
			@RequestParam("tag_group_code") String tagGroupCode,
			@RequestParam("tag_group_name") String tagGroupName) {
		
		List<Map<String, Object>> items = this.tagGroupService.getTagGroupResult(tagGroupCode, tagGroupName);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/detail")
	private AjaxResult getTagGroupResultDetail(
			@RequestParam(value = "id", required = false) Integer id) {
		
		Map<String, Object> items = this.tagGroupService.getTagGroupResultDetail(id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@PostMapping("/save")
	private AjaxResult saveTagGroupResult(
			@RequestParam(value = "id", required = false) Integer id,
			//@RequestParam(value = "fileId", required = false) Integer fileId,
			@RequestParam(value = "tag_group_code", required = false) String tag_group_code,
			@RequestParam(value = "tag_group_name", required = false) String tag_group_name,
			@RequestParam(value = "description", required = false) String description,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		TagGroup tg = null;
		
		if ( id == null) {
			tg = new TagGroup();
		} else {
			tg = this.tagGroupRepository.getTagGroupById(id);
		}
		tg.setCode(tag_group_code);
		tg.setName(tag_group_name);
		tg.setDescription(description);
		tg.set_audit(user);
		
		tg = this.tagGroupRepository.save(tg);
		
		
		result.data = tg;
		
		return result;
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteTagGroupResult(
			@RequestParam(value = "id", required = false) Integer id) {
		
		AjaxResult result = new AjaxResult();
		
		this.tagGroupRepository.deleteById(id);
		
		return result;
	}
	
}
