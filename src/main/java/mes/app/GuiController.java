package mes.app;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import mes.app.common.TenantContext;
import mes.domain.entity.MenuItem;
import mes.domain.repository.MenuItemRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

import mes.config.Settings;
import mes.domain.entity.MenuUseLog;
import mes.domain.entity.User;
import mes.domain.model.GUIData;
import mes.domain.repository.MenuUseLogRepository;
import mes.domain.services.SqlRunner;

@Controller
public class GuiController {

	@Autowired
	Settings settings;

	@Autowired
	SqlRunner sqlRunner;

	@Autowired
	MenuUseLogRepository menuUseLogRepository;

	@Autowired
	MenuItemRepository menuItemRepository;

	private ModelAndView getView(String gui, String templateName, User user, MultiValueMap<String, String> allRequestParams) {
		ModelAndView mv = new ModelAndView();

		// menuItemRepository → JPA → mainDataSource (항상 메인 DB)
		MenuItem menuItem = menuItemRepository.findByMenuCode(gui);

		if (menuItem != null && menuItem.getTemplate() != null) {
			String username = user.getUserProfile().getName();

			for (String k : allRequestParams.keySet()) {
				mv.addObject(k, allRequestParams.get(k).get(0));
			}

			// "default" 템플릿은 DB의 template 컬럼 사용, 그 외는 templateName을 직접 뷰 경로로 사용
			String templatePath = "default".equals(templateName) ? menuItem.getTemplate() : templateName;
			mv.setViewName(templatePath);

			mv.addObject("username", username);
			mv.addObject("userinfo", user);
			mv.addObject("gui_code", gui);
			mv.addObject("template_key", templateName);

			String mqtt_host = settings.getProperty("mqtt_host");
			String mqtt_web_port = settings.getProperty("mqtt_web_port");
			String hmi_topic = settings.getProperty("hmi_topic");
			mv.addObject("mqtt_host", mqtt_host);
			mv.addObject("mqtt_web_port", mqtt_web_port);
			mv.addObject("hmi_topic", hmi_topic);

			// 권한처리
			// admin 계정이거나 super_user 플래그가 있으면 모든 권한 부여 (getWebMenuList 와 동일 기준)
			boolean isAdmin = "admin".equals(user.getUsername()) || Boolean.TRUE.equals(user.getSuperUser());

			boolean read_flag = false;
			boolean write_flag = false;

			if (!isAdmin) {
				// user_group_menu 는 Main DB 테이블, spjangcd 와 무관하게 그룹+메뉴 기준으로 조회
				// (getWebMenuList 와 동일한 방식)
				MapSqlParameterSource dicParam = new MapSqlParameterSource();
				dicParam.addValue("MenuCode", gui);
				dicParam.addValue("UserGroupId", user.getUserProfile().getUserGroup().getId());

				String sql = """
                        select "AuthCode" from user_group_menu ugm
                        where ugm."MenuCode" = :MenuCode and "UserGroup_id" = :UserGroupId
                        """;

				Map<String, Object> map = this.sqlRunner.getRow(sql, dicParam);

				if (map == null) {
					mv.setViewName("errors/403");
					return mv;
				}

				String authCode = map.get("AuthCode").toString();
				if (authCode.isEmpty()) {
					mv.setViewName("errors/403");
					return mv;
				}

				read_flag = authCode.contains("R");
				write_flag = authCode.contains("W");
			} else {
				read_flag = true;
				write_flag = true;
			}

			mv.addObject("read_flag", read_flag);
			mv.addObject("write_flag", write_flag);

			// 메뉴 접속 로그 기록
			if ("default".equals(templateName)) {
				try {
					String tenantId = user.getSpjangcd();
					if (tenantId == null) tenantId = TenantContext.get();

					MenuUseLog menuUseLog = new MenuUseLog();
					menuUseLog.setMenuCode(gui);
					menuUseLog.setUserId(user.getId());
					menuUseLog.set_audit(user);
					menuUseLog.setSpjangcd(tenantId);

					this.menuUseLogRepository.save(menuUseLog);
				} catch (Exception ex) {
					System.out.println("MenuUseLog save" + ex);
				}
			}

		} else {
			mv.setViewName("errors/404");
		}

		return mv;
	}

	@GetMapping("/gui/{gui}")
	public ModelAndView pageGUI(
			@PathVariable("gui") String gui,
			@RequestParam MultiValueMap<String, String> allRequestParams
	) {
		SecurityContext sc = SecurityContextHolder.getContext();
		Authentication auth = sc.getAuthentication();
		User user = (User) auth.getPrincipal();

		return this.getView(gui, "default", user, allRequestParams);
	}

	@GetMapping("/gui/{gui}/{template}")
	public ModelAndView pageGUITemplate(
			@PathVariable("gui") String gui,
			@PathVariable("template") Optional<String> template,
			@RequestParam MultiValueMap<String, String> allRequestParams,
			Authentication auth
	) {
		User user = (User) auth.getPrincipal();

		String templateName = template.orElse("default");
		return this.getView(gui, templateName, user, allRequestParams);
	}

	@GetMapping("/page/{folder}/{template}")
	public ModelAndView pageTemplatePathView(
			@PathVariable("folder") String folder,
			@PathVariable("template") String template,
			Authentication auth
	) {
		User user = (User) auth.getPrincipal();

		ModelAndView mv = new ModelAndView();
		mv.addObject("userinfo", user);
		mv.setViewName(String.format("page/%s/%s", folder, template));

		return mv;
	}
}
