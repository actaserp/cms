package mes.app.tagdata;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.tagdata.service.TagCurrentService;
import mes.domain.model.AjaxResult;


@RestController
@RequestMapping("/api/tagdata/tag_current")
public class TagCurrentController {
	
	@Autowired
	TagCurrentService tagCurrentService;
	
	
	@GetMapping("/read")
	public AjaxResult getTagCurrentList(
			@RequestParam("action")String action) {
       
        List<Map<String, Object>> items = this.tagCurrentService.getTagCurrentList(action);      
               		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
}
