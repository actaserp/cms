package mes.app.system;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import mes.domain.entity.MenuFrontFolder;
import mes.domain.repository.MenuFrontFolderRepository;
import mes.domain.services.SqlRunner;
import org.apache.groovy.parser.antlr4.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import mes.app.system.service.MenuSetupService;
import mes.domain.entity.MenuFolder;
import mes.domain.entity.MenuItem;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.MenuFolderRepository;
import mes.domain.repository.MenuItemRepository;
import mes.domain.services.CommonUtil;
@RestController
@RequestMapping("/api/system/menu_setup")
public class MenuSetupController {

	@Autowired
	private MenuSetupService menuSetupService;

	@Autowired
	MenuFolderRepository menuFolderRepository;

	@Autowired
	MenuItemRepository menuItemRepository;

	@Autowired
	MenuFrontFolderRepository menuFrontFolderRepository;

	@Autowired
	SqlRunner mainSqlRunner;

	// 메뉴폴더) 리스트 조회
	@GetMapping("/read")
	public AjaxResult getFolderTreeList() {
		List<Map<String, Object>> items = this.menuSetupService.getFolderTreeList();

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	// 메뉴항목) 리스트 조회
	@GetMapping("/submenu_list")
	public AjaxResult getMenuList(
			@RequestParam(value = "folder_id") Integer folder_id) {
		List<Map<String, Object>> items = this.menuSetupService.getMenuList(folder_id);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	// 소스메뉴) 리스트 조회
	@GetMapping("/gui_use_list")
	public AjaxResult getGuiUseList(
			@RequestParam(value = "unset", required = false) String unset,
			@RequestParam(value = "keyword", required = false) String keyword) {
		List<Map<String, Object>> items = this.menuSetupService.getGuiUseList(unset, keyword);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	// 메뉴폴더) 순서저장
	@PostMapping("/folder_order_save")
	@Transactional
	public AjaxResult saveFolderOrder(
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {

		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();

		List<Integer> items = loadJsonList(Q.getFirst("Q").toString());

		if (items.size() == 0) {
			result.success = false;
			return result;
		}

		Integer order = 10;

		for (int i = 0; i < items.size(); i++) {

			MenuFolder menuFolder = null;

			Integer id = items.get(i);
			menuFolder = this.menuFolderRepository.getMenuFolderById(id);

			if (menuFolder != null) {

				menuFolder.setOrder(order);
				menuFolder.set_audit(user);

				this.menuFolderRepository.save(menuFolder);

				order = order + 10;
			}
		}

		return result;
	}

	// 메뉴폴더) 폴더추가
	@PostMapping("/folder_insert")
	@Transactional
	public AjaxResult insertFolder(
			@RequestParam(value = "old_id", required = false) Integer old_id,
			@RequestParam(value = "FolderName", required = false) String FolderName,
			@RequestParam(value = "IconCSS", required = false) String IconCSS,
			HttpServletRequest request,
			Authentication auth) {

		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();

		if (StringUtils.isEmpty(IconCSS)) {
			IconCSS = "fa-vials";
		}

		Integer _order = null;

		if (old_id != null) {

			MenuFolder mf = this.menuFolderRepository.getMenuFolderById(old_id);

			if (mf != null && mf.getOrder() != null) {
				_order = mf.getOrder() + 1;
			}
		}

		MenuFolder menu_folder = new MenuFolder();

		menu_folder.setFolderName(FolderName);
		menu_folder.setIconCss(IconCSS);
		menu_folder.setOrder(_order);
		menu_folder.set_audit(user);

		this.menuFolderRepository.save(menu_folder);

		return result;
	}

	// 메뉴폴더) 이름변경
	@PostMapping("/folder_name_save")
	@Transactional
	public AjaxResult saveFolderName(
			@RequestParam(value = "id", required = false) Integer id,
			@RequestParam(value = "FolderName", required = false) String FolderName,
			@RequestParam(value = "IconCSS", required = false) String IconCSS,
			HttpServletRequest request,
			Authentication auth) {

		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();

		MenuFolder menu_folder = this.menuFolderRepository.getMenuFolderById(id);

		if (menu_folder != null) {

			menu_folder.setFolderName(FolderName);
			menu_folder.setIconCss(IconCSS);
			menu_folder.set_audit(user);

			this.menuFolderRepository.save(menu_folder);
		} else {
			result.success = false;
		}
		return result;
	}

	// 메뉴폴더) 삭제
	@PostMapping("/folder_delete")
	@Transactional
	public AjaxResult deletefolder(@RequestBody Map<String, Object> body) {

		AjaxResult result = new AjaxResult();

		Integer Folder_id = body.get("Folder_id") != null
				? Integer.valueOf(body.get("Folder_id").toString()) : null;

		List<MenuItem> menu_folder = this.menuItemRepository.findByMenuFolderId(Folder_id);

		if (menu_folder != null && menu_folder.size() > 0) {
			result.success = false;
			result.message = "하위 메뉴가 존재합니다.";
		} else {
			this.menuFolderRepository.deleteById(Folder_id);
		}

		return result;
	}

	// 메뉴항목) 메뉴추가 (폴더에 메뉴 할당)
	@PostMapping("/menu_save")
	@Transactional
	public AjaxResult saveMenu(
			@RequestParam(value = "Folder_id", required = false) Integer Folder_id,
			@RequestParam(value = "MenuCode", required = false) String MenuCode,
			HttpServletRequest request,
			Authentication auth) {

		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();

		MenuItem menu_item = this.menuItemRepository.findByMenuCode(MenuCode);

		if (menu_item == null) {
			Integer order_no = 0;
			menu_item = new MenuItem();

			List<MenuItem> dic_max = this.menuItemRepository.findByMenuFolderId(Folder_id);

			if (dic_max != null && dic_max.size() > 0) {
				Collections.sort(dic_max, (d1, d2) -> {
					int o1 = d1.getOrder()==null?0:d1.getOrder();
					int o2 = d2.getOrder()==null?0:d2.getOrder();
					return o2 - o1;
				});
				if (dic_max.get(0).getOrder() != null) {
					order_no = dic_max.get(0).getOrder();
				}
			}

			order_no = order_no + 10;
			menu_item.setUrl("/gui/" + MenuCode);
			menu_item.setOrder(order_no);
			menu_item.setMenuCode(MenuCode);
			menu_item.setMenuName(MenuCode);
		}

		menu_item.setMenuFolderId(Folder_id);
		menu_item.set_audit(user);
		this.menuItemRepository.save(menu_item);

		return result;
	}

	// 메뉴항목) 메뉴삭제
	@PostMapping("/menu_delete")
	@Transactional
	public AjaxResult deleteMenu(
			@RequestBody MultiValueMap<String,Object> menus,
			HttpServletRequest request) {

		AjaxResult result = new AjaxResult();

		List<Map<String, Object>> arr_menus = CommonUtil.loadJsonListMap(menus.getFirst("menus").toString());

		for (int i = 0; i < arr_menus.size(); i++) {

			String MenuCode = arr_menus.get(i).get("MenuCode").toString();
			Integer Folder_id = Integer.parseInt(arr_menus.get(i).get("Folder_id").toString());

			MenuItem menuItem = this.menuItemRepository.findByMenuFolderIdAndMenuCode(Folder_id, MenuCode);

			if (menuItem != null) {
				this.menuItemRepository.delete(menuItem);
			}
		}

		return result;
	}

	// 메뉴항목) 메뉴저장
	@PostMapping("/menu_list_save")
	@Transactional
	public AjaxResult saveMenuList(
			@RequestParam(value = "Folder_id", required = false) Integer Folder_id,
			@RequestBody MultiValueMap<String,Object> menus,
			HttpServletRequest request,
			Authentication auth) {

		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();

		List<Map<String, Object>> arr_menus = CommonUtil.loadJsonListMap(menus.getFirst("menus").toString());

		for (int i = 0; i < arr_menus.size(); i++) {

			String MenuCode = arr_menus.get(i).get("MenuCode").toString();
			String MenuName = arr_menus.get(i).get("MenuName").toString();
			Integer MenuFolder_id = Integer.parseInt(arr_menus.get(i).get("MenuFolder_id").toString());
			Integer _order = Integer.parseInt(arr_menus.get(i).get("_order").toString());

			MenuItem menuItem = this.menuItemRepository.findByMenuFolderIdAndMenuCode(MenuFolder_id, MenuCode);

			if (menuItem != null) {

				menuItem.setMenuName(MenuName);
				menuItem.setMenuFolderId(MenuFolder_id);
				menuItem.set_audit(user);
				menuItem.setUrl("/gui/" + MenuCode + "/default");
				menuItem.setOrder(_order);

				this.menuItemRepository.save(menuItem);
			} else {
				result.success = false;
			}
		}

		return result;
	}

	// ── Front Folder ──────────────────────────────────────────────

	@GetMapping("/front_folder_read")
	public AjaxResult getFrontFolderList() {
		AjaxResult result = new AjaxResult();
		result.data = menuFrontFolderRepository.findAllByOrderByOrderAsc();
		return result;
	}

	@PostMapping("/front_folder_save")
	@Transactional
	public AjaxResult saveFrontFolder(@RequestBody Map<String, Object> body, Authentication auth) {
		AjaxResult result = new AjaxResult();
		User user = (User) auth.getPrincipal();

		Integer id = body.get("id") != null ? Integer.valueOf(body.get("id").toString()) : null;

		MenuFrontFolder folder = (id != null)
				? menuFrontFolderRepository.findById(id).orElse(new MenuFrontFolder())
				: new MenuFrontFolder();

		folder.setFolder_name((String) body.get("folder_name"));
		folder.setIcon_css((String) body.get("icon_css"));
		folder.setDom_id((String) body.get("dom_id"));
		folder.set_order(body.get("_order") != null ? Integer.valueOf(body.get("_order").toString()) : null);
		folder.setUse_yn(body.getOrDefault("use_yn", "Y").toString());
		folder.set_audit(user);
		menuFrontFolderRepository.save(folder);
		return result;
	}

	@PostMapping("/front_folder_delete")
	@Transactional
	public AjaxResult deleteFrontFolder(@RequestBody Map<String, Object> body) {
		AjaxResult result = new AjaxResult();
		Integer id = Integer.valueOf(body.get("id").toString());
		MapSqlParameterSource p = new MapSqlParameterSource();
		p.addValue("id", id);
		Map<String, Object> row = mainSqlRunner.getRow(
				"SELECT COUNT(*) AS cnt FROM menu_folder WHERE \"FrontFolder_id\"=:id", p);
		long cnt = row != null ? ((Number) row.get("cnt")).longValue() : 0;
		if (cnt > 0) {
			result.success = false;
			result.message = "하위 폴더가 존재합니다.";
			return result;
		}
		menuFrontFolderRepository.deleteById(id);
		return result;
	}

	// ── Folder (FrontFolder_id 포함) ──────────────────────────────

	@PostMapping("/folder_save")
	@Transactional
	public AjaxResult saveFolder(@RequestBody Map<String, Object> body, Authentication auth) {
		AjaxResult result = new AjaxResult();
		User user = (User) auth.getPrincipal();

		Integer id = body.get("id") != null ? Integer.valueOf(body.get("id").toString()) : null;
		String folderName    = (String) body.get("FolderName");
		String iconCSS       = body.get("IconCSS") != null ? body.get("IconCSS").toString() : "fa-folder";
		Integer frontFolderId = body.get("frontFolderId") != null && !body.get("frontFolderId").toString().isBlank()
				? Integer.valueOf(body.get("frontFolderId").toString()) : null;
		Integer order        = body.get("_order") != null ? Integer.valueOf(body.get("_order").toString()) : null;

		if (StringUtils.isEmpty(iconCSS)) iconCSS = "fa-folder";

		MenuFolder folder = (id != null) ? menuFolderRepository.getMenuFolderById(id) : new MenuFolder();
		if (folder == null) { result.success = false; return result; }

		folder.setFolderName(folderName);
		folder.setIconCss(iconCSS);
		folder.setFrontFolderId(frontFolderId);
		folder.setOrder(order);
		folder.set_audit(user);
		menuFolderRepository.save(folder);
		return result;
	}

	// ── Menu Item 신규 생성 ────────────────────────────────────────

	@PostMapping("/menu_item_save")
	@Transactional
	public AjaxResult saveMenuItem(@RequestBody Map<String, Object> body, Authentication auth) {
		AjaxResult result = new AjaxResult();
		User user = (User) auth.getPrincipal();

		String menuCode  = (String) body.get("MenuCode");
		String menuName  = (String) body.get("MenuName");
		String template  = (String) body.get("template");
		Integer folderId = body.get("Folder_id") != null && !body.get("Folder_id").toString().isBlank()
				? Integer.valueOf(body.get("Folder_id").toString()) : null;
		Integer order    = body.get("_order") != null ? Integer.valueOf(body.get("_order").toString()) : 10;
		String iconCSS   = (String) body.get("IconCSS");

		MenuItem item = menuItemRepository.findByMenuCode(menuCode);
		if (item == null) item = new MenuItem();

		item.setMenuCode(menuCode);
		item.setMenuName(menuName);
		item.setTemplate(template);
		item.setMenuFolderId(folderId);
		item.setOrder(order);
		item.setIconCSS(iconCSS);
		item.setUrl("/gui/" + menuCode);
		item.set_audit(user);
		menuItemRepository.save(item);
		return result;
	}

	@PostMapping("/menu_item_delete")
	@Transactional
	public AjaxResult deleteMenuItem(@RequestBody Map<String, Object> body) {
		AjaxResult result = new AjaxResult();
		MenuItem item = menuItemRepository.findByMenuCode((String) body.get("MenuCode"));
		if (item != null) menuItemRepository.delete(item);
		return result;
	}

	private static List<Integer> loadJsonList(String strJson) {

		ObjectMapper objectMapper = new ObjectMapper();
		List<Integer> result = null;
		try {
			result = objectMapper.readValue(strJson, new TypeReference<List<Integer>>(){});
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}

		return result;
	}
}
