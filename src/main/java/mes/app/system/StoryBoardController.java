package mes.app.system;

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

import mes.app.system.service.StoryBoardService;
import mes.domain.entity.StoryBoardItem;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.StoryBoardRepository;

@RestController
@RequestMapping("/api/system/storyboard")
public class StoryBoardController {

	@Autowired
	StoryBoardService storyBoardService;
	
	@Autowired
	StoryBoardRepository storyBoardRepository;
	
	@GetMapping("/read")
	public AjaxResult getStoryboardItemList() {
		List<Map<String, Object>> items = this.storyBoardService.getStoryboardItemList();
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	@PostMapping("/save_menu")
	public AjaxResult saveStoryBoardMenu(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam("BoardType") String BoardType,
			@RequestParam("MenuCode") String MenuCode,
			@RequestParam("Duration") Integer Duration,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		StoryBoardItem sbi = null;
		
		if (id==null) {
			sbi = new StoryBoardItem();
		} else {
			sbi = this.storyBoardRepository.getStoryBoardById(id);
		}
		
		String url = "/gui/"+ MenuCode;
	
		sbi.setMenuCode(MenuCode);
		sbi.setDuration(Duration);
		sbi.setBoardType(BoardType);
		sbi.setUrl(url);
		sbi.set_audit(user);
		sbi = this.storyBoardRepository.save(sbi);
		
		AjaxResult result = new AjaxResult();
        result.data = sbi;
		return result;
		
	}
	
	@PostMapping("/save_hmi")
	public AjaxResult saveStoryBoardHmi(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam("BoardType") String BoardType,
			@RequestParam("MenuCode") String MenuCode,
			@RequestParam("Duration") Integer Duration,
			@RequestParam("ParameterData") String ParameterData,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		StoryBoardItem sbi = null;
		
		if (id==null) {
			sbi = new StoryBoardItem();
		} else {
			sbi = this.storyBoardRepository.getStoryBoardById(id);
		}
		
		String url = "/gui/" + MenuCode + "/detail?pk=" + ParameterData;
		
		sbi.setMenuCode(MenuCode);
		sbi.setDuration(Duration);
		sbi.setBoardType(BoardType);
		sbi.setParameterData(ParameterData);
		sbi.setUrl(url);
		sbi.set_audit(user);
		sbi = this.storyBoardRepository.save(sbi);
		
		AjaxResult result = new AjaxResult();
        result.data = sbi;
		return result;
		
	}
	
	@PostMapping("delete")
	public AjaxResult deleteStoryBoard(
			@RequestParam("id_list") String id_list) {
		String [] list = id_list.split(",");
		AjaxResult result = new AjaxResult();
		for(String id:list) {
			this.storyBoardRepository.deleteById(Integer.parseInt(id));
		}
		return result;
	}
}
