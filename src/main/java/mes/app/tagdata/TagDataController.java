package mes.app.tagdata;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.tagdata.service.TagDataService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/tagdata/tagdata_list")
public class TagDataController {
	
	@Autowired
	TagDataService tagDataService;
	
	
	/**
	 * @apiNote 품목조회
	 * @param  MatType, 품목구분
	 * @param matGroupId 품목그룹pk
	 * @param keyword 키워드
	 * @return
	 */
	@GetMapping("/read")
	public AjaxResult getMaterialList(
			@RequestParam("data_date") String data_date, 
    		@RequestParam("start_time") String start_time,
    		@RequestParam("end_time") String end_time,
    		@RequestParam("tag_code") String tag_code) {
		
		String data_from = data_date +" "+ start_time + ":00";
		String data_to = data_date + " "+ end_time + ":00";
		
       
        List<Map<String, Object>> items = this.tagDataService.getTagDataList(data_from, data_to, tag_code);      
               		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	
	
}
