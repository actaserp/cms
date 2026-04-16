package mes.app.tagdata;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import mes.app.tagdata.service.TagTrendService;
import mes.domain.model.AjaxResult;



@RestController
@RequestMapping("/api/tagdata/tag_trend")
public class TagTrendController {
	@Autowired
	TagTrendService tagTrendService;
	
	
	@PostMapping("/read")
	public AjaxResult getTagTrendList(
			@RequestParam("start_time") String start_time,
			@RequestParam("end_time") String end_time,
			@RequestParam("tag_codes") String tag_codes) {
		
		List<Map<String,Object>> items = this.tagTrendService.getTagTrendList(start_time,end_time,tag_codes);
		
		AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
		
	}

}
