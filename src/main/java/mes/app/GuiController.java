package mes.app;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
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
import mes.domain.GUIConfiguration;
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
	
	private ModelAndView getView(String gui, String templateName, User user, MultiValueMap<String, String> allRequestParams) {
		ModelAndView mv = new ModelAndView();
        GUIData guiData= GUIConfiguration.getGUIInfo(gui);
        if (guiData!=null) {
        	String username = user.getUserProfile().getName();
        	
        	for(String k : allRequestParams.keySet()){
        	     mv.addObject(k, allRequestParams.get(k).get(0));
        	}
        	
            // get 파라미터를 뷰에 넘겨줘야함
        	//mv.addAllObjects(allRequestParams);
        	
        	String templatePath = guiData.templates.get(templateName);
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

    		// 권한처리 루틴 시작
    		// 메뉴별 권한 조회
    		Map<String,Object> map = new HashMap<String,Object>();

			// /gui/ 요청은 SpjangSecurityInterceptor가 TenantContext를 세팅하지 않으므로
			// 인증된 User 객체에서 직접 spjangcd를 가져온다 (세션 의존 제거)
			String tenantId = user.getSpjangcd();
			if (tenantId == null) {
				tenantId = TenantContext.get(); // fallback
			}

			MapSqlParameterSource dicParam = new MapSqlParameterSource();			
			dicParam.addValue("MenuCode", gui);
			dicParam.addValue("spjangcd", tenantId);
			dicParam.addValue("UserGroupId", user.getUserProfile().getUserGroup().getId());
			dicParam.addValue("UserGroupId", user.getUserProfile().getUserGroup().getId());
			
			String sql = """
						select "AuthCode" from user_group_menu ugm
						where ugm."MenuCode" = :MenuCode and "UserGroup_id" = :UserGroupId and spjangcd = :spjangcd
						""";
			
			map = this.sqlRunner.getRow(sql, dicParam);
			
			String active = null;
			
			boolean read_flag = false; 
			boolean write_flag = false;

			boolean isAdmin = "admin".equals(user.getUsername());

			if (map != null) {
				active = map.get("AuthCode").toString();

				// AuthCode가 빈값이면 권한 없는 것으로 처리
				if (active.isEmpty()) {
					if (isAdmin) {
						read_flag = true;
						write_flag = true;
					} else {
						mv.setViewName("errors/403");
						return mv;
					}
				} else {
					read_flag = active.contains("R");
					write_flag = active.contains("W");
				}
			} else if (isAdmin) {
				read_flag = true;
				write_flag = true;
			} else {
				mv.setViewName("errors/403");
				return mv;
			}
			
			// 권한 셋팅
    		mv.addObject("read_flag", read_flag);
    		mv.addObject("write_flag", write_flag);
    		
            // 메뉴 접속 로그 기록 저장
            if ("default".equals(templateName)) {
            	try {
    	        	MenuUseLog menuUseLog = new MenuUseLog();
    	        	menuUseLog.setMenuCode(gui);
    	        	menuUseLog.setUserId(user.getId());
    	        	menuUseLog.set_audit(user);
					menuUseLog.setSpjangcd(tenantId);
    	        	
    	            this.menuUseLogRepository.save(menuUseLog);
            	} catch(Exception ex) {
            		System.out.println("MenuUseLog save" + ex);
            	}
            }
            
        }
        else {
        	//404 page 이동
        	mv.setViewName("errors/404");
        }       
        
		return mv;
	}
	
	@GetMapping("/gui/{gui}")
	public ModelAndView pageGUI(
			@PathVariable("gui")String gui, 
			@RequestParam MultiValueMap<String, String> allRequestParams
			) {
		
        SecurityContext sc = SecurityContextHolder.getContext();
        Authentication auth = sc.getAuthentication();         
        User user = (User)auth.getPrincipal();        
        //System.out.println(user.getUsername());		
        

        String templateName = "default";       
        ModelAndView mv = this.getView(gui, templateName, user, allRequestParams);
		return mv;
	}
	
	@GetMapping("/gui/{gui}/{template}")
	public ModelAndView pageGUITemplate(
			@PathVariable("gui")String gui, 
			@PathVariable("template")Optional<String> template,
			@RequestParam MultiValueMap<String, String> allRequestParams,
			Authentication auth
			) {	
        
        User user = (User)auth.getPrincipal();        		
        
        String templateName = "default";
        if(template.isPresent()) {
        	templateName = template.get();
        }
        
        ModelAndView mv = this.getView(gui, templateName, user, allRequestParams);
		return mv;
	}
	
	@GetMapping("/page/{folder}/{template}")
	public ModelAndView pageTemplatePathView(
			@PathVariable("folder")String folder,
			@PathVariable("template")String template,
			Authentication auth
			) {
		User user = (User)auth.getPrincipal();
		
		ModelAndView mv = new ModelAndView();
		mv.addObject("userinfo", user);
		
		String viewName = String.format("/page/%s/%s", folder, template);
		mv.setViewName(viewName);		
		
		return mv;
		
	}
}