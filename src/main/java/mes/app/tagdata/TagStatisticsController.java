package mes.app.tagdata;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.tagdata.service.TagStatisticsService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/tagdata/tag_statistics")
public class TagStatisticsController {
	
	@Autowired
	TagStatisticsService tagStatisticsService;
	
	
	
	@GetMapping("/read")
	public AjaxResult getStatisticsList(
			@RequestParam(value="start_date", required=false) String start_date, 
    		@RequestParam(value="end_date",required=false) String end_date,
    		@RequestParam(value="tag_code",required=false) String tag_code,
    		@RequestParam(value="tag_group_pk",required=false) Integer tag_group_pk,
    		HttpServletRequest request) {
		
       
        List<Map<String, Object>> items = this.tagStatisticsService.getStatisticsList(start_date, end_date, tag_code,tag_group_pk);      
               		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}

}
