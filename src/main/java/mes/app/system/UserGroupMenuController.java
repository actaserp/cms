package mes.app.system;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpSession;
import javax.transaction.Transactional;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.system.service.SystemService;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/system/usergroupmenu")
public class UserGroupMenuController {

	@Autowired
	SystemService systemService;
	
	@Autowired
	SqlRunner sqlRunner;	
	
	
	@GetMapping("/read")
	public AjaxResult getUserGroupMenuList(
			@RequestParam(value="group_id", required = false) Integer groupId,
			@RequestParam(value="folder_id", required = false) Integer folderId,
			HttpSession session
			) {

		String loginId = (String) session.getAttribute("userid");

		AjaxResult result = new AjaxResult();
		result.data = this.systemService.getUserGroupMenuList(groupId, folderId, loginId);
		return result;		
	}
	
	@PostMapping("/save")
	@Transactional
    public AjaxResult saveUserGroupMenus(
    		@RequestParam(value="group_id") Integer groupId,
    		@RequestParam(value="Q") String strUserGroupMenuList,
    		Authentication auth
    		) {
		AjaxResult result = new AjaxResult();
		User user = (User)auth.getPrincipal();
		
		List<Map<String, Object>> userGroupMenuList = CommonUtil.loadJsonListMap(strUserGroupMenuList);
		
	
		userGroupMenuList.forEach(map->{
			String menuCode =(String)map.get("menu_code");
			Integer ugm_id =(Integer)map.get("ugm_id");
			
			boolean r = Boolean.parseBoolean(map.get("r").toString());
			boolean w = Boolean.parseBoolean(map.get("w").toString());
			boolean a = Boolean.parseBoolean(map.get("a").toString());

			String authCode = "";
			if(r) {
				authCode="R";
			}
			if(w) {
				authCode+="W";
			}
			if (a) authCode += "A";
			
			MapSqlParameterSource dicParam = new MapSqlParameterSource();
			String tenantId = TenantContext.get();
			dicParam.addValue("spjangcd", tenantId);
			dicParam.addValue("auth_code", authCode);
			dicParam.addValue("menu_code", menuCode);
			dicParam.addValue("group_id", groupId);
			dicParam.addValue("user_id", user.getId());

			if(ugm_id==null) {
				if(authCode.isEmpty()) return; // 권한 없고 새거면 insert 안함

				// insert
				String sql = """
				insert into user_group_menu("UserGroup_id", "MenuCode", "AuthCode", _creater_id, _created, spjangcd)
				values(:group_id, :menu_code, :auth_code, :user_id, now(), :spjangcd)
				""";
				this.sqlRunner.execute(sql, dicParam);
			} else {
				if(authCode.isEmpty()) {
					// 권한 모두 해제 → delete
					dicParam.addValue("id", ugm_id);
					String sql = "delete from user_group_menu where id=:id";
					this.sqlRunner.execute(sql, dicParam);
				} else {
					// update
					dicParam.addValue("id", ugm_id);
					String sql = """
					update user_group_menu set "UserGroup_id"=:group_id, "MenuCode"=:menu_code, "AuthCode"=:auth_code, _modifier_id=:user_id, _modified=now(), spjangcd=:spjangcd
					where id=:id
					""";
					this.sqlRunner.execute(sql, dicParam);
				}
			}
		});	
		
		return result;		
	}	
}

