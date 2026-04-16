package mes.app.system.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class StoryBoardService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getStoryboardItemList() {
		
		String sql = """
		    select si.id
            , case when si."BoardType" ='menu' 
                then concat(mi."MenuName",'(', mf."FolderName",')')
               else df."FormName" end as name
              , si."BoardType"
              , fn_code_name('story_board_type', si."BoardType" ) as "BoardTypeName"
              , si."Duration"
              , si."Url"
              , up."Name" as writer
              , to_char(si."_created" ,'yyyy-mm-dd hh24:mi:ss') as created
              , si."ParameterData"
            from storyboard_item si 
            left join menu_item mi on mi."MenuCode" = si."MenuCode" 
            left join menu_folder mf on mf.id = mi."MenuFolder_id" 
            left join doc_form df on df.id = si."ParameterData"::int
            left join user_profile up on up."User_id" = si."_creater_id"   
            order by id     
        """;
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql,null);
		
		return items;
	}

}
