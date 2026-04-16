package mes.app.system.service;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;

import mes.domain.entity.User;
import mes.domain.entity.UserGroup;
import mes.domain.services.SqlRunner;

@Repository
public class SystemService {

	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
    private NamedParameterJdbcTemplate  jdbcTemplate;


	public List<Map<String, Object>> getWebMenuList(User user) {

		String sql = """
        with recursive tree(id, menu_code, pid, name, depth, path, cycle, folder_order, _order, css, data_div, folder_id, FrontFolder_id) as (
        select 
            a.id,
            null as menu_code,
            a."Parent_id" as pid,
            a."FolderName" as name, 
            1 as depth,
            array[a.id] as path,
            false as cycle,
            a._order as folder_order,
            a._order,
            a."IconCSS"::text as css,
            'folder' as data_div,
            a.id as folder_id,
            a."FrontFolder_id"
        from 
            menu_folder a 
        where 
            a."Parent_id" is null
        
        union all 
        
        select  
            null as id,
            mi."MenuCode"::text as menu_code,
            mi."MenuFolder_id" as pid,
            mi."MenuName" as name,
            tree.depth + 1,
            array_append(tree.path, mi."MenuFolder_id") as path,
            mi."MenuFolder_id" = any(tree.path) as cycle,
            tree.folder_order,
            mi._order,
            null as css,
            'menu' as data_div,
            mi."MenuFolder_id" as folder_id,
            tree.FrontFolder_id
        from 
            menu_item mi 
        inner join 
            tree on mi."MenuFolder_id" = tree.id 
        where 
            (:super_user = true)
            or (exists (
                select 1 
                from user_group_menu gm 
                where gm."MenuCode" = mi."MenuCode" 
                and gm."UserGroup_id" = :group_id
                and (gm."AuthCode" like '%R%' OR gm."AuthCode" LIKE '%W%')
            ))
        and 
            not cycle
        ), M as (
            select 
                tree.id,
                tree.menu_code,
                tree.pid,
                tree.name,
                tree.depth,
                tree.folder_order,
                tree._order,
                coalesce(tree.css,'') as css,
                (bk."MenuCode" is not null) as isbookmark,
                tree.data_div,
                count(*) over (partition by tree.folder_id) as sub_count,
                tree.path,
                tree.FrontFolder_id
            from 
                tree 
            left join 
                bookmark bk on bk."MenuCode" = tree.menu_code 
            and 
                bk."User_id" = :user_id 
            where 
                1 = 1
        )
        select 
            id, 
            menu_code, 
            pid, 
            name, 
            depth, 
            folder_order, 
            _order, 
            css, 
            isbookmark,
            case when depth = 1 then FrontFolder_id else null end as FrontFolder_id
        from 
            M
        where 
            sub_count > 1
        and 
            FrontFolder_id is not null
        order by 
            depth, _order;
        """;



		UserGroup userGroup = user.getUserProfile().getUserGroup();

		String group_code = userGroup.getCode();
		boolean super_user = user.getSuperUser();

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("group_id", userGroup.getId());
		dicParam.addValue("user_id", user.getId());
		dicParam.addValue("group_code", group_code);
		dicParam.addValue("super_user", super_user);
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

		return items;

	}

	/**
	 * 시스템-메뉴권한 : 사용자그룹별 메뉴조회
	 * @param userGroupId
	 * @param folderId
	 * @return
	 */
	public List<Map<String, Object>> getUserGroupMenuList(Integer userGroupId, Integer folderId, String loginId) {

		String sql = """
                with recursive tree as (  
                        select a.id
                            , a."Parent_id" as pid
                            , '' as menu_code
                            , a."FolderName" as gpname
                            , a."FolderName" as name
                            , 1 as depth
                            , array[a.id] as path
                            , false as cycle
                            , a."_order" as ord
                            ,'folder' as data_div
                            , a.id as folder_id
                            , true as is_folder
                            from menu_folder a   
                            where a."Parent_id" is null
                             and a."FrontFolder_id" is not null
                """;

		if (!"admin".equals(loginId)) {
			sql += " and a.\"FrontFolder_id\" != 15";
		}

		if (folderId != null) {
			sql += " and a.id = :folder_id";
		}

		sql += """
                      union all
                              select null as id
                                    , mi."MenuFolder_id" as pid
                                    , mi."MenuCode"::text as menu_code
                                    , tree."gpname" as gpname
                                    , mi."MenuName"  as name
                                    , tree.depth+1
                                    , array_append(tree.path, mi."MenuFolder_id") as path
                                    , mi."MenuFolder_id" = any(tree.path) as cycle
                                    , mi._order as ord
                                    ,'menu' as data_div
                                    , mi."MenuFolder_id" as folder_id
                                    , false as is_folder
                              from menu_item mi
                              inner join tree on mi."MenuFolder_id" = tree.id
                        )
                        select tree.pid
                            , tree.id
                            , tree.menu_code
                            , tree.gpname
                            , tree.name
                            , tree.depth
                            , tree.ord
                            , ugm."UserGroup_id"
                            , ugm."AuthCode"
                            , case when tree.is_folder then null else coalesce(ugm."AuthCode" like '%%R%%', false) end  as r
                            , case when tree.is_folder then null else coalesce(ugm."AuthCode" like '%%W%%', false) end  as w
                            , case when tree.is_folder then null else coalesce(ugm."AuthCode" like '%%A%%', false) end  as a
                            , case when tree.is_folder then null else coalesce(ugm."AuthCode" like '%%X%%', false) end  as x
                            , tree.is_folder
                            , ugm.id as ugm_id
                        from tree 
                        left join user_group_menu ugm on ugm."MenuCode" = tree.menu_code 
                        and ugm."UserGroup_id" = :group_id
                        order by path, tree.ord						
                """;

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("folder_id", folderId);
		dicParam.addValue("group_id", userGroupId);
		dicParam.addValue("loginId", loginId);
		return this.sqlRunner.getRows(sql, dicParam);
	}

	public List<Map<String, Object>> getBookmarkList(int userId){
		
		String sql="""
        select mi."MenuName" as name 
        , mi."MenuCode" as code 
        , mi."Url" as url
        --, false as ismanual
        --, 0 as ismanual
	    , bm."_created" as created
        from bookmark bm 
        inner join menu_item mi on bm."MenuCode" = mi."MenuCode" 
        where bm."User_id" = :user_id
        order by created
		""";
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();

        dicParam.addValue("user_id", userId);
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);        
        return items;
	}
	
	public int saveBookmark(String menucode, String isbookmark, User user) {
		int iRowEffected = 0;
		
		MapSqlParameterSource namedParameters = new MapSqlParameterSource();
		namedParameters.addValue("menucode", menucode);
		namedParameters.addValue("user_id", user.getId());

		if ("false".equals(isbookmark)) {
			// isbookmark가 'false'일 경우, 북마크 추가
			String sql = """
                insert into bookmark ("User_id", "MenuCode", _created) values(:user_id, :menucode, now())
                """;
			iRowEffected = this.jdbcTemplate.update(sql, namedParameters);
		} else {
			// isbookmark가 'true'일 경우, 북마크 삭제
			String sql = """
                delete from bookmark where "User_id"=:user_id and "MenuCode"=:menucode
                """;
			iRowEffected = this.jdbcTemplate.update(sql, namedParameters);
		}    
		
		return iRowEffected;
	}
	
	public List<Map<String, Object>> getLabelList(String lang_code, String gui_code, String template_key){
		
		String sql = """
		        select
		        lc."ModuleName" as gui_code
		        , lc."TemplateKey" as template_key
		        , lc."LabelCode" as label_code
		        , lc."Description" as descr
		        , lcl."DispText" as text
		        from label_code lc 
		        left join label_code_lang lcl on lc.id = lcl."LabelCode_id" 
		        where lcl."LangCode" = :lang_code 
		        """;
		        if (StringUtils.hasText(gui_code)) {
		            sql +="""	            		
		           and lc."ModuleName" = :gui_code and lc."TemplateKey"=:template_key 
		           """;		            
		        }
		        sql += """ 
		        order by lc."ModuleName", lc."TemplateKey" 
		""";
		        
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("lang_code", lang_code);
        dicParam.addValue("gui_code", gui_code);
        dicParam.addValue("template_key", template_key);
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
		        
	}
	
	
	public List<Map<String, Object>> getGridColumnList(String moduleName, String templateName, String gridName, String langCode){
		
		String sql = """
            select gc."Index" as index2, gc."Key" as "key"
            , gc."Label" as src_label
            , gc."Width" as width
            , case when gc."Hidden" = 'Y' then 'true' else 'false' end as hidden
	        , coalesce(cl."DispText", gc."Label") as label
	        , cl."DispText" as text
	        from grid_col gc 
	        left join grid_col_lang cl on cl."GridColumn_id" = gc.id
	        and cl."LangCode" = :lang_code
	        where gc."ModuleName" = :module_name
	        and gc."TemplateKey" = :template_name
	        and gc."GridName" = :grid_name
	        order by gc."Index"
		""";
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("module_name", moduleName);
        dicParam.addValue("template_name", templateName);
        dicParam.addValue("grid_name", gridName);
        dicParam.addValue("lang_code", langCode);
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
		return items;
	}

	public List<Map<String, Object>> getSystemLogList(Timestamp start, Timestamp end, String type, String source) {
		String sql = """
        SELECT 
            ROW_NUMBER() OVER (ORDER BY sl."_created" DESC) AS row_num,
            sl.id,
            sl."Type" AS type,
            sl."Source" AS source,
            sl."Message" AS message,
            TO_CHAR(sl."_created", 'yyyy-mm-dd hh24:mi:ss') AS created
        FROM sys_log sl
        WHERE sl."_created" BETWEEN :start AND :end
    """;

		if (StringUtils.hasText(type)) {
			sql += " AND sl.\"Type\" LIKE CONCAT('%', :type, '%') ";
		}

		if (StringUtils.hasText(source)) {
			sql += " AND sl.\"Source\" LIKE CONCAT('%', :source, '%') ";
		}

		sql += " ORDER BY sl.\"_created\" DESC";

		MapSqlParameterSource namedParameters = new MapSqlParameterSource();
		namedParameters.addValue("start", start, java.sql.Types.TIMESTAMP);
		namedParameters.addValue("end", end, java.sql.Types.TIMESTAMP);
		namedParameters.addValue("type", type);
		namedParameters.addValue("source", source);

		return this.jdbcTemplate.queryForList(sql, namedParameters);
	}


	public Map<String, Object> getSystemLogDetail(Long id){
    	String sql = """
            select id, "Type" as type, "Source" as source,"Message" as message
            , to_char("_created" ,'yyyy-mm-dd hh24:mi:ss') as created
            from sys_log sl
            where id = :log_id    			
    	""";
    	MapSqlParameterSource namedParameters = new MapSqlParameterSource();
    	namedParameters.addValue("log_id", id);
    	
    	return this.jdbcTemplate.queryForMap(sql, namedParameters);    	
    }
    
//    @Cacheable(value="grid_column",key="{#moduleName, #templateName, #gridName, #key}")    
//    public Map<String, Object> getGridColums(String moduleName, String templateName, String gridName, String key){
//    	
//    	String sql = """    			
//        select  id, "ModuleName", "TemplateKey", "GridName", "Key", "Index", "Label", "Width", "Hidden"
//        , _status, _created, _modified, _creater_id, _modifier_id
//        from public.grid_col
//    	where  "ModuleName" = :moduleName and "TemplateKey" = :templateName and "GridName" = :gridName and "Key" = :key
//    	""";
//    	
//    	MapSqlParameterSource namedParameters = new MapSqlParameterSource();
//    	namedParameters.addValue("moduleName", moduleName);
//    	namedParameters.addValue("templateName", templateName);
//    	namedParameters.addValue("gridName", gridName);
//    	namedParameters.addValue("key", key);
//    	
//    	Map<String, Object> items = this.sqlRunner.getRow(sql, namedParameters);
//    	//return this.jdbcTemplate.queryForObject(sql, namedParameters, GridColumn.class);
//    	return items;
//    }
    
//    @CachePut(value = "grid_column",key="{#gridColumn.moduleName, #gridColumn.templateName, #gridColumn.gridName, #key}")
//    public GridColumn saveGridColumn(GridColumn gridColumn) {
//    	
//    	MapSqlParameterSource namedParameters = new MapSqlParameterSource();
//    	namedParameters.addValue("id", gridColumn.getId());
//		namedParameters.addValue("ModuleName", gridColumn.getModuleName());
//		namedParameters.addValue("TemplateKey", gridColumn.getTemplateKey());
//		namedParameters.addValue("GridName", gridColumn.getGridName());
//		namedParameters.addValue("Key", gridColumn.getKey());
//		namedParameters.addValue("Index", gridColumn.getIndex());
//		namedParameters.addValue("Label", gridColumn.getLabel());
//		namedParameters.addValue("Width", gridColumn.getWidth());
//		namedParameters.addValue(":Hidden", gridColumn.getHidden());
//		
//		namedParameters.addValue(":_status", gridColumn.get_status());
//		namedParameters.addValue(":_created", gridColumn.get_created());
//		namedParameters.addValue(":_modified", gridColumn.get_modified());
//		namedParameters.addValue(":_creater_id", gridColumn.get_creater_id());
//		namedParameters.addValue(":_modifier_id", gridColumn.get_modifier_id());		
//    	
//    	if(gridColumn.getId()==null) {
//    		
//    		String sql = """
//            INSERT INTO grid_col("ModuleName", "TemplateKey", "GridName", "Key", "Index", "Label", "Width", "Hidden", "_status", "_created", "_modified", "_creater_id", "_modifier_id",)
//            VALUES(
//            :ModuleName
//            , :TemplateKey
//            , :GridName
//            , :Key
//            , :Index
//            , :Label
//            , :Width
//            , :Hidden
//            , :_status
//            , :_created
//            , :_modified
//            , :_creater_id
//            , :_modifier_id
//            )
//            """;
//    		
//    		KeyHolder keyHolder = new GeneratedKeyHolder();
//        	this.jdbcTemplate.update(sql, namedParameters, keyHolder, new String[] {"id"});    	
//        	Integer id = keyHolder.getKey().intValue();
//        	gridColumn.setId(id);
//    		
//    	}else {
//    		String sql = """
//            UPDATE grid_col SET
//            "_status"=:_status
//            , "_created"=:_created
//            , "_modified"=:_modified
//            , "_creater_id"=:_creater_id
//            , "_modifier_id"=:_modifier_id
//            , "ModuleName"=:ModuleName
//            , "TemplateKey"=:TemplateKey
//            , "GridName"=:GridName
//            , "Key"=:Key
//            , "Index"=:Index
//            , "Label"=:Label
//            , "Width"=:Width
//            , "Hidden"=:Hidden
//            WHERE id=:id 			
//    		""";
//    		
//    		this.jdbcTemplate.update(sql, namedParameters);
//    	}    	
//    	return gridColumn;
//    }
    
//    @CacheEvict(value = "grid_column", key="{#moduleName, #templateName, #gridName}")
//    public int deleteGridColumns(String moduleName, String templateName, String gridName) {
//    	int iRowEffected = 0;
//    	
//    	String sql = """
//    	delete from grid_col where "ModuleName"=:ModuleName and "TemplateKey"=:TemplateKey and "GridName"=:GridName
//    	""";
//    	
//    	MapSqlParameterSource namedParameters = new MapSqlParameterSource();    	
//    	namedParameters.addValue("ModuleName", moduleName);
//    	namedParameters.addValue("TemplateKey", templateName);
//    	namedParameters.addValue("GridName", gridName);
//    	
//    	iRowEffected = this.jdbcTemplate.update(sql, namedParameters);    	
//    	return iRowEffected;
//    }

	public Map<String, Object> getLabelCodeLangDetail(String guiCode, String labelCode, String langCode,
			String templateKey) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("guiCode", guiCode);
		paramMap.addValue("labelCode", labelCode);
		paramMap.addValue("langCode", langCode);
		paramMap.addValue("templateKey", templateKey);
		
		String sql = """
				select lc.id as lable_code_id
                , lcl.id as label_lang_id
                , lc."ModuleName"
                , coalesce ((select "MenuName" from menu_item where "MenuCode"=lc."ModuleName" limit 1), 'Common') as menu_name
                , lc."TemplateKey"
                , lc."LabelCode"
                , lc."Description"
                , lcl."LangCode"
                , lcl."DispText"
                , to_char(lcl."_created" ,'yyyy-mm-dd hh24:mi:ss') as disp_created
                from label_code_lang lcl
                inner join label_code lc on lcl."LabelCode_id" = lc.id
                where lcl."LangCode" = :langCode
                and lc."ModuleName" = :guiCode
                and lc."TemplateKey" = :templateKey
                and lc."LabelCode" = :labelCode
				""";
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
		return item;
	}

	public List<Map<String, Object>> storyBoard() {
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
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, null);
		
		return items;
	}
}