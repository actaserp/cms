package mes.app.system;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.servlet.http.HttpServletRequest;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.system.service.UserGroupService;
//import mes.app.system.usergroup.service.UserGroupService;
import mes.domain.entity.User;
import mes.domain.entity.UserGroup;
import mes.domain.model.AjaxResult;
import mes.domain.repository.UserGroupRepository;

@RestController
@RequestMapping("/api/system/usergroup")
public class UserGroupController{
	
	@Autowired
	UserGroupRepository userGroupRepository;
	
	@Autowired 
	private UserGroupService userGroupService;
	
	
	@GetMapping("/read")
	public AjaxResult getUserGroupList(
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		Boolean super_user = user.getSuperUser();
		String spjangcd = TenantContext.get();

		if (super_user == false ) {
			super_user = Objects.equals(user.getUserProfile().getUserGroup().getCode(), "dev");
		}			
				
		
		List<Map<String,Object>> items = this.userGroupService.getUserGroupList(super_user,spjangcd);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
		
	}

	@GetMapping("/detail")
	public AjaxResult getUserGroup(@RequestParam("id") int id) {
	
		Map<String, Object> item = this.userGroupService.getUserGroup(id);
		
		AjaxResult result = new AjaxResult();
		result.data = item;
		return result;
	}
	
	@PostMapping("/save")
	public AjaxResult saveUserGroup(
			@RequestParam(value="id", required= false) Integer id,
			@RequestParam("code") String code,
			@RequestParam("name") String name,
			@RequestParam ("description") String description,
			@RequestParam ("gmenu") String gmenu,
			@RequestParam(value ="spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth) {
			User user = (User)auth.getPrincipal();
			
			UserGroup ug=null;
			
			if(id == null) {
				ug = new UserGroup();
			} else {
				ug = this.userGroupRepository.getUserGrpById(id);
			}
			
			ug.setName(name);
			ug.setCode(code);
			ug.setDescription(description);
			ug.set_audit(user);
			ug.setSpjangcd(spjangcd);
			ug.setGmenu(gmenu);

			ug = this.userGroupRepository.save(ug);
			
			AjaxResult result = new AjaxResult();
			result.data = ug;
			
			return result;
			
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteUserGroup(@RequestParam("id") Integer id) {
		this.userGroupRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		
		return result;
	}

	@GetMapping("/defaultmenu")
	public AjaxResult getUserGroup() {

		SecurityContext sc = SecurityContextHolder.getContext();
		Authentication auth = sc.getAuthentication();
		User user = (User)auth.getPrincipal();

		Map<String, Object> item = this.userGroupService.getDefaultMenu(user);

		AjaxResult result = new AjaxResult();
		result.data = item;
		return result;
	}
	
	
}