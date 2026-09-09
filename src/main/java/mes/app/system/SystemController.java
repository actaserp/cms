package mes.app.system;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpSession;

import lombok.extern.slf4j.Slf4j;
import mes.app.common.TenantContext;
import mes.app.system.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.system.service.SystemService;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.services.SqlRunner;

@RequestMapping("/api/system")
@RestController
@Slf4j
public class SystemController {

	@Autowired
	SystemService systemService;

	@Autowired
	UserService userService;

	@Autowired
	SqlRunner sqlRunner;

	//@Cacheable(value = "menus",  key = "#auth.name")
	@SuppressWarnings("unchecked")
	@GetMapping("/menus")
	public AjaxResult menus(Authentication auth) {

		User user = (User) auth.getPrincipal();

		List<Map<String, Object>> items = this.systemService.getWebMenuList(user);
		List<Map<String, Object>> frontFolders = this.systemService.getFrontFolderList();
		if (items == null) items = new ArrayList<>();
		if (frontFolders == null) frontFolders = new ArrayList<>();

		Map<Integer, Object> nodeMap = new HashMap<Integer, Object>();
		List<Map<String, Object>> menuItems = new ArrayList<>();

		for(int idx=0; idx<items.size();idx++) {
			Map<String, Object> dicData = items.get(idx);
			Integer id = (Integer)dicData.get("id");
			String menu_code=(String)dicData.get("menu_code");
			Integer pid = (Integer)dicData.get("pid");
			String name=(String)dicData.get("name");
			boolean isbookmark = (boolean)dicData.get("isbookmark");
			String css = (String)dicData.get("css");

			Integer frontfolder_id = (Integer) dicData.get("frontfolder_id");

			if (id!=null) {
				List<Map<String, Object>> nodes = new ArrayList<Map<String, Object>>();

				Map<String, Object> folder = new HashMap<>();
				folder.put("folder_id", id);
				folder.put("folder_name", name);
				folder.put("frontfolder_id", frontfolder_id);
				folder.put("nodes", nodes);
				folder.put("ismanual", false);
				folder.put("isbookmark", false);
				folder.put("menuDepth", 1);

				menuItems.add(folder);
				nodeMap.put(id, nodes);
			}
			else {
				String url = String.format("/gui/%s", menu_code);
				List<Map<String, Object>> nodes =(ArrayList<Map<String, Object>>)nodeMap.get(pid);

				Map<String, Object> menuItem = new HashMap<>();
				menuItem.put("objId", menu_code);
				menuItem.put("objNm", name);
				menuItem.put("objUrl", url);
				menuItem.put("ismanual", false);
				menuItem.put("isbookmark", isbookmark);
				menuItem.put("menuDepth", 2);

				nodes.add(menuItem);
			}

		}

		Map<String, Object> resultData = new HashMap<>();
		resultData.put("menuItems", menuItems);
		resultData.put("frontFolders", frontFolders);

		AjaxResult result = new AjaxResult();
		result.success = true;
		result.data = resultData;

		return result;
	}

	@GetMapping("/bookmark")
	public AjaxResult bookmark() {

		SecurityContext sc = SecurityContextHolder.getContext();
		Authentication auth = sc.getAuthentication();
		User user = (User)auth.getPrincipal();

		List<Map<String, Object>> items = this.systemService.getBookmarkList(user.getId());
		AjaxResult result = new AjaxResult();
		result.data = items;
		result.success = true;
		return result;
	}

	@PostMapping("/bookmark/save")
	public AjaxResult bookmarkSave(
			@RequestParam(value="menucode") String menucode,
			@RequestParam(value="isbookmark", required = false) String isbookmark,
			Authentication auth
	) {

		User user = (User)auth.getPrincipal();
		AjaxResult result = new AjaxResult();
		result.data = this.systemService.saveBookmark(menucode, isbookmark, user);
		result.success = true;
		return result;
	}

	@GetMapping("/storyboard")
	public AjaxResult storyBoard() {

		List<Map<String, Object>> items = this.systemService.storyBoard();

		AjaxResult result = new AjaxResult();
		result.data = items;
		result.success = true;
		return result;
	}

	private static final String START_TIME = String.valueOf(System.currentTimeMillis());

	@GetMapping("/api/system/version")
	public Map<String, String> getVersion() {
		return Map.of("version", START_TIME);
	}

	/**
	 * 사업장 전환 (슈퍼유저 전용).
	 *
	 * 파라미터명이 spjangcd 가 아니라 target_spjangcd 인 이유:
	 *   SpjangSecurityInterceptor 는 /api/** 요청의 spjangcd 파라미터가 세션 값과
	 *   다르면 403 으로 막는다. 전환은 본질적으로 "다른 값"을 보내는 요청이므로
	 *   spjangcd 라는 이름을 쓰면 자기 자신이 인터셉터에 걸린다.
	 *   인터셉터(테넌트 격리 방어선)를 손대는 대신 파라미터명을 분리했다.
	 */
	@PostMapping("/switch-spjang")
	public AjaxResult switchSpjang(
			@RequestParam("target_spjangcd") String targetSpjangcd,
			Authentication auth,
			HttpSession session) {

		AjaxResult result = new AjaxResult();
		User user = (User) auth.getPrincipal();

		// 슈퍼유저가 아니면 무조건 거부 — 일반 사용자는 전환 자체가 불가능하다.
		if (!Boolean.TRUE.equals(user.getSuperUser())) {
			log.warn("[사업장전환 거부] 권한없음 user={} target={}", user.getUsername(), targetSpjangcd);
			result.success = false;
			result.message = "권한이 없습니다.";
			return result;
		}

		// 존재하지 않는 코드로 세션이 오염되는 것을 막는다.
		if (!this.userService.existsSpjang(targetSpjangcd)) {
			log.warn("[사업장전환 거부] 미존재 user={} target={}", user.getUsername(), targetSpjangcd);
			result.success = false;
			result.message = "존재하지 않는 사업장입니다.";
			return result;
		}

		String before = (String) session.getAttribute("spjangcd");
		session.setAttribute("spjangcd", targetSpjangcd);
		TenantContext.set(targetSpjangcd);

		// 감사 로그 — 누가 언제 어느 사업장에 들어갔는지 추적 가능해야 한다.
		log.warn("[사업장전환] user={} {} -> {}", user.getUsername(), before, targetSpjangcd);

		Map<String, Object> data = new HashMap<>();
		data.put("spjangcd", targetSpjangcd);
		result.data = data;
		result.success = true;
		return result;
	}

}