package mes.app;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import mes.app.notification.NotificationService;
import mes.app.system.service.UserService;
import mes.domain.entity.Notification;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;

import mes.config.Settings;
import mes.domain.entity.SystemOption;
import mes.domain.entity.User;
import mes.domain.repository.SystemOptionRepository;


@Controller
public class HomeController {
	
	@Autowired
	SystemOptionRepository systemOptionRepository;
	
	@Autowired
	Settings settings;

	@Autowired
	UserService userService;

	@Autowired
	NotificationService notificationService;

	@RequestMapping(value= "/", method=RequestMethod.GET)
    public ModelAndView pageIndex(HttpServletRequest request, HttpSession session, HttpServletResponse response) {

		// User-Agent 확인
		String userAgent = request.getHeader("User-Agent").toLowerCase();
		boolean isMobile = userAgent.contains("mobile") || userAgent.contains("android") || userAgent.contains("iphone");

		String serverName = request.getServerName();

//		if (isMobile && serverName.equalsIgnoreCase("actascld.co.kr")) {
//			String redirectUrl = "https://mes.actascld.co.kr";
//			try {
//				response.sendRedirect(redirectUrl);
//				return null; // redirect 했으므로 이후 처리 중단
//			} catch (IOException e) {
//				e.printStackTrace(); // 로그로 출력하거나, 에러 뷰로 포워딩도 가능
//				return new ModelAndView("error/redirect_error"); // 예외 시 fallback 처리
//			}
//		}

        SecurityContext sc = SecurityContextHolder.getContext();
        Authentication auth = sc.getAuthentication();         
        User user = (User)auth.getPrincipal();
        String username = user.getUserProfile().getName();
		String userid = user.getUsername();
		Integer groupid = user.getUserProfile().getUserGroup().getId();
		String groupname = user.getUserProfile().getUserGroup().getName();
		String spjangcd = user.getSpjangcd();
                
        SystemOption sysOpt= this.systemOptionRepository.getByCode("LOGO_TITLE");
        String logoTitle = sysOpt.getValue();
        
        //q = this.systemOptionRepository.getByCode("main_menu");        

		List<Map<String, Object>> spjangList = null;
		if (groupid == 1){ //관리자
			spjangList = userService.getSpjangList();
		} else {
			spjangList = userService.getSpjang(spjangcd);
		}

		ModelAndView mv = new ModelAndView();
		session.setAttribute("spjangList", spjangList);
		mv.addObject("username", username);
		mv.addObject("userid", userid);
		mv.addObject("groupname", groupname);
		session.setAttribute("spjangcd", spjangcd);
		session.setAttribute("username", username);
		session.setAttribute("userid", userid);
		session.setAttribute("groupid", groupid);
		mv.addObject("userinfo", user);
		mv.addObject("system_title", logoTitle);
		mv.addObject("default_menu_code", "wm_dashboard_summary");

		boolean readFlag = true;
		boolean writeFlag = true; // 유저의 실제 권한에 따라 true/false 처리

		// ModelAndView에 객체 추가
		mv.addObject("userinfo", user); // 이미 객체는 넘기고 계시네요.
		mv.addObject("read_flag", readFlag);  // JS의 userinfo.read_flag와 매칭
		mv.addObject("write_flag", writeFlag); // JS의 userinfo.write_flag와 매칭
		mv.addObject("gui_code", "MAIN");

		// 안읽은 알람
		List<Notification> unreadList = notificationService.getUnread(userid, spjangcd);
		mv.addObject("unreadAlarmCount", unreadList.size());
		mv.addObject("unreadAlarmList", unreadList);

		mv.setViewName(isMobile ? "mobile/mobile_main" : "index");
		
		return mv;
	}

	@RequestMapping(value= "/intro", method=RequestMethod.GET)
    public ModelAndView pageIntro(HttpServletRequest request, HttpSession session) {
		ModelAndView mv = new ModelAndView();
		mv.setViewName("intro");
		return mv;
	}
	

	@RequestMapping(value= "/setup", method=RequestMethod.GET)
	public ModelAndView pageSetup(Authentication auth, HttpServletResponse response) throws IOException {
		
		// 로그아웃된 상태인 경우 로그인페이지로 이동
		if (auth == null) {
		    response.sendRedirect("/login");
			return null;
		} 
		
		User user = (User)auth.getPrincipal();
		String username = user.getUserProfile().getName();
		
		ModelAndView mv = new ModelAndView();
		mv.addObject("username", username);
		mv.addObject("userinfo", user);
		
		mv.setViewName("/system/setup");
		return mv;
	}
	
		
	
}