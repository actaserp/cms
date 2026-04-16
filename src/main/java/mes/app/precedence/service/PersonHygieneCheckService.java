package mes.app.precedence.service;

import java.io.File;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import mes.config.Settings;
import mes.domain.entity.CheckItemResult;
import mes.domain.entity.CheckResult;
import mes.domain.entity.User;
import mes.domain.repository.CheckItemResultRepository;
import mes.domain.repository.CheckResultRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@Service
public class PersonHygieneCheckService {

	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	CheckResultRepository checkResultRepository;
	
	@Autowired
	CheckItemResultRepository checkItemResultRepository;
	
	@Autowired
	Settings settings;
	
	// 결재현황 조회
	public List<Map<String, Object>> getPersonCheckItemApprStatus(String startDate, String endDate, String apprState, String diaryType) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("startDate", startDate);
		paramMap.addValue("endDate", endDate);
		paramMap.addValue("apprState", apprState);
		paramMap.addValue("diaryType", diaryType);
        
		String sql = """
				select b.id, b."Char1" as "Title", coalesce(r."StateName", '작성') as "StateName"
				, r."LineName", r."LineNameState", to_char(b."Date1", 'yyyy-MM-dd') as "DataDate"
				, coalesce(r."SearchYN", 'Y') as "SearchYN", coalesce(r."EditYN", 'Y') as "EditYN"
				, coalesce(r."DeleteYN", 'Y') as "DeleteYN", b."Number1" as check_master_id
				, b._creater_id ,up."Name" as "creater_name" , b._modifier_id, up2."Name" as "modifier_name"
				, cm."CheckCycle" 
				, b."Text1" as diary_type
				from bundle_head b                               
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				left join user_profile up on b._creater_id = up."User_id"
				left join user_profile up2 on b._modifier_id = up2."User_id"
				left join check_mast cm on cm.id = b."Number1"
				where b."TableName" = 'person_hygiene_check'
				and b."Date1" between cast(:startDate as date) and cast(:endDate as date)
				""";
		
		if (!apprState.isEmpty() && apprState != null) {
            if (apprState.equals("write")) {
            	sql += " and r.\"State\" is null ";
            } else {
            	sql += " and r.\"State\" = :apprState ";
            }
		}
		
		if (!diaryType.isEmpty() && diaryType != null) {
            	sql += " and b.\"Text1\" = :diaryType ";
		}
		
		sql += " order by b.\"Date1\" desc ";
  			    
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}
	
	// 점검표 조회
	public Map<String, Object> getPersonCheckItemList(Integer bh_id, String diary_type) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("bh_id", bh_id);
        
        Map<String,Object> head_info = new HashMap<String,Object>();;
        List<Map<String, Object>> diary_info = new ArrayList<Map<String, Object>>();
        Map<String,Object> items = new HashMap<String,Object>();
        
    	String sql = """
    		select b.id, b."Char1" as "Title"
    		, to_char(b."Date1", 'yyyy-MM-dd') as "DataDate"
    		,  coalesce(uu."Name", cu."Name") as "FirstName"
    		, coalesce(r."State", 'write') as "State"
    		, coalesce(r."StateName", '작성') as "StateName"
    		, b."Text1" as diary_type
			from bundle_head b
			inner join user_profile cu on b._creater_id = cu."User_id"
			left join user_profile uu on b._modifier_id = uu."User_id"
			left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
			where b."TableName" = 'person_hygiene_check'
			and b.id = :bh_id
        	""";
    	
  		//일지 헤더	    
        head_info = this.sqlRunner.getRow(sql, dicParam);
        
        if(diary_type.equals("개인위생일지(직원)")) {
         sql = """
    		select cr.id as id
			, cr."Number1" as dept_id
			, cr."TargetName" as dept
			, cr."Checker_id" as worker_id
			, cr."CheckerName" as worker
			, MAX(CASE WHEN t2.rn = 1 THEN t2."Result1" END) AS item0
			, MAX(CASE WHEN t2.rn = 2 THEN t2."Result1" END) AS item1
			, MAX(CASE WHEN t2.rn = 3 THEN t2."Result1" END) AS item2
			, MAX(CASE WHEN t2.rn = 4 THEN t2."Result1" END) AS item3
			, MAX(CASE WHEN t2.rn = 5 THEN t2."Result1" END) AS item4
			, MAX(CASE WHEN t2.rn = 6 THEN t2."Result1" END) AS item5
			, 'Y' as sign_state
			, cr."Char1" as signature
			, cr."Char2" as day_off
			from check_result cr 
			left join (
				select cir.id, cir."CheckResult_id" ,cir."CheckItem_id" ,cir."Result1"
				, row_number() over (partition by cir."CheckResult_id" order by ci."_order") as rn
				from check_item_result cir 
				inner join check_item ci on ci.id = cir."CheckItem_id"
				left join check_result cr on cr.id = cir."CheckResult_id"
				order by cir."CheckResult_id" ,cir."CheckItem_id"
			) t2 on cr.id = t2."CheckResult_id"
			where cr."SourceDataPk" = :bh_id
			GROUP BY 
			  cr.id,
			  cr."Number1",
			  cr."TargetName",
			  cr."Checker_id",
			  cr."CheckerName",
			  cr."Char1",
			  cr."Char2"
			order by cr.id
        	""";
        }else {
        	sql = """
        		select cr.id as id
    			, cr."TargetName" as day_getin
    			, cr."CheckerName" as worker
    			, MAX(CASE WHEN t2.rn = 1 THEN t2."Result1" END) AS item0
    			, MAX(CASE WHEN t2.rn = 2 THEN t2."Result1" END) AS item1
    			, MAX(CASE WHEN t2.rn = 3 THEN t2."Result1" END) AS item2
    			, MAX(CASE WHEN t2.rn = 4 THEN t2."Result1" END) AS item3
    			, MAX(CASE WHEN t2.rn = 5 THEN t2."Result1" END) AS item4
    			, MAX(CASE WHEN t2.rn = 6 THEN t2."Result1" END) AS item5
    			, 'Y' as sign_state
    			, cr."Char1" as signature
    			from check_result cr 
    			left join (
    				select cir.id, cir."CheckResult_id" ,cir."CheckItem_id" ,cir."Result1"
    				, row_number() over (partition by cir."CheckResult_id" order by ci."_order") as rn
    				from check_item_result cir 
    				inner join check_item ci on ci.id = cir."CheckItem_id"
    				left join check_result cr on cr.id = cir."CheckResult_id"
    				order by cir."CheckResult_id" ,cir."CheckItem_id"
    			) t2 on cr.id = t2."CheckResult_id"
    			where cr."SourceDataPk" = :bh_id
    			GROUP BY 
    			  cr.id,
    			  cr."Number1",
    			  cr."TargetName",
    			  cr."Checker_id",
    			  cr."CheckerName",
    			  cr."Char1",
    			  cr."Char2"
    			order by cr.id
            	""";
        }
         
         diary_info = this.sqlRunner.getRows(sql, dicParam);
         
         sql = """
     		select da.id, cir.id as check_result_id
			, da."HappenDate" as happen_date, da."HappenPlace" as happen_place
			, da."AbnormalDetail" as abnormal_detail, da."ActionDetail" as action_detail
			, da."ActionState" as action_state, da."ConfirmState" as confirm_state
			, da."SourceDataPk" as src_data_pk, da."SourceTableName" as src_table_name
			, up."Name" as actor_name
			, up2."Name" as creater_name
			, right(da."AbnormalDetail", strpos(':', reverse(da."AbnormalDetail"))-1) as check_name
			, cir._order as _order, ci._order as index_order
			from "devi_action" da
			inner join "check_item_result" cir on cir.id = da."SourceDataPk" and da."SourceTableName" = 'person_hygiene_check'
			inner join "check_result" cr on cr.id = cir."CheckResult_id" and cr."SourceDataPk" = :bh_id
			inner join "check_item" ci on cir."CheckItem_id" = ci.id
			left join "user_profile" up on up."User_id" = da._modifier_id
			left join "user_profile" up2 on up2."User_id" = da._creater_id
			order by ci._order
        		""";

        List<Map<String, Object>> action_info = this.sqlRunner.getRows(sql, dicParam);

		sql = """
				select ers."StudentName" , ers.id from edu_result_student ers
				where ers."SourceDataPk"  = :bh_id
				and ers."SourceTableName" = 'bundle_head'
			  """;
		
		List<Map<String,Object>> studentInfo = this.sqlRunner.getRows(sql, dicParam);
        
		items.put("head_info", head_info);
        items.put("diary_info", diary_info);
        items.put("action_info", action_info);
        items.put("student_info", studentInfo);
        
        return items;
	}
	
	//점검표 삭제
	public void deletePersonCheckItemList(Integer bhId) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		
		String sql = null;
		
		sql = """
            delete from check_item_result                     
            where "CheckResult_id" in (
                select id from check_result 
                where "SourceDataPk" = :bhId
            )
            and "CheckItem_id" in (
	            select distinct ci.id 
	            from check_result cr 
	            inner join check_item  ci
	            on cr."CheckMaster_id" = ci."CheckMaster_id"
	            where cr."SourceDataPk" = :bhId
                )  
			""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		
		sql = """
	            delete from check_result 
	            where "SourceDataPk" = :bhId
			  """;
		
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
				 delete from devi_action
                where "SourceDataPk" not in (
	                select b.id
	                from check_result a
	                inner join check_item_result b on a.id = b."CheckResult_id" and b."Result1" = 'X'
	                left join  devi_action c on c."SourceDataPk" = b.id
	                where 1=1	
                )
                and "SourceTableName" = 'person_hygiene_check'
			  """;
		
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
			   delete from bundle_head where id = :bhId
			  """;
		
		this.sqlRunner.execute(sql, paramMap);
		
	}
	
	
	
	
	public List<Map<String, Object>> getPersonLoginInfo(String loginId) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("loginId", loginId);  
        
        String sql = """
        		select id as person_id
	            , "Name" as person_name
	            from person
	            where "LoginID" = :loginId
        		""";
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
	}

	public List<Map<String, Object>> getCheckItemList(String personId, String dataDate, Integer masterId) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("personId", personId);  
        dicParam.addValue("dataDate", dataDate);
        dicParam.addValue("masterId", masterId);
        
        String sql = """
	  		select cr.id
		    from check_result cr
		    where cr."CheckMaster_id" = :masterId
		    and cr."CheckDate" = cast(:dataDate as date)
		    and cr."SourceDataPk" = cast(:personId as Integer)
		    and cr."SourceTableName" = 'person'
        	""";
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
	}

	public List<Map<String, Object>> getCheckItemListFirst(String id) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("id", id); 
		String sql = """
				select ci.id as check_item_id
	            , ci."Name" as check_item_name
	            , cir."Result1" as result 
                , cir."CheckResult_id" as cr_id
                , pd."Number1" as temperature
	            from check_item_result cir
	            inner join check_item ci on ci.id = cir."CheckItem_id"
                left join prop_data pd on pd."DataPk" = cir."CheckResult_id" and pd."TableName" = 'CheckResult' and pd."Code" = 'Temperature'
	            where cir."CheckResult_id" = cast(:id as Integer)
	            order by cir."_order" 
				""";
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
	}

	public List<Map<String, Object>> getCheckItemListSecond(Integer masterId, String dataDate) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("masterId", masterId); 
        dicParam.addValue("dataDate", dataDate); 
		String sql = """
				select ci.id as check_item_id
	            , ci."Name" as check_item_name
                , '' as result
	            from check_item ci
	            where ci."CheckMaster_id" = :masterId
                and cast(:dataDate as date) between ci."StartDate" and ci."EndDate"
                order by _order
				""";
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
	}

	public Integer saveCheckResultAndReturnId(int index, Map<String, Object> row, Integer bhId, Integer check_master_id, String data_date,
            String diary_type, User user) {
		
		List<CheckResult> crList = this.checkResultRepository.findBySourceDataPkOrderByIdAsc(bhId);
	    CheckResult cr;
	    // 수정 시, 새로운 행이 추가 되었는지 판단
	    if (crList.size() > index) {
	    	// crList에서 OrderByIdAsc 했기 때문에 crList.get(index) 가능
	        cr = crList.get(index);
	    } else {
	    	// 수정 시, 새로운 행이 추가 되었을 때
	        cr = new CheckResult();
	    }
	    
		cr.setCheckMasterId(check_master_id);
		cr.setCheckDate(Date.valueOf(data_date));
		cr.setCheckerName(CommonUtil.tryString(row.get("worker")));
		cr.setChar1(CommonUtil.tryString(row.get("signature")));
		
		if (diary_type.equals("개인위생일지(직원)")) {
			cr.setSourceTableName("bundle_head-person");
			cr.setTargetName(CommonUtil.tryString(row.get("dept")));
			cr.setCheckerId(CommonUtil.tryInt(CommonUtil.tryString(row.get("worker_id"))));
			cr.setNumber1(CommonUtil.tryInt(CommonUtil.tryString(row.get("dept_id"))));
			cr.setChar2(CommonUtil.tryString(row.get("day_off")));
		} else {
			cr.setSourceTableName("bundle_head-person_exterior");
			cr.setTargetName(CommonUtil.tryString(row.get("day_getin")));
		}
		
		cr.setSourceDataPk(bhId);
		cr.set_audit(user);
		cr = this.checkResultRepository.save(cr);
		return cr.getId();
	}
			
	public void saveCheckItemResult(Map<String, Object> colItem, Integer check_result_id, User user) {
		
		Integer check_item_id = CommonUtil.tryInt(colItem.get("id"));
		String item_result = CommonUtil.tryString(colItem.get("result"));
		
		CheckItemResult cir = this.checkItemResultRepository.findByCheckResultIdAndCheckItemId(check_result_id, check_item_id);
		if (cir == null) {
			cir = new CheckItemResult();
		}
		cir.setCheckResultId(check_result_id);
		cir.setCheckItemId(check_item_id);
		cir.setResult1(item_result);
		cir.set_audit(user);
		this.checkItemResultRepository.save(cir);
	}
	
	// sign 업로드
	public void uploadSign(Integer dataPk, String param, User user) throws IOException {
		
		String sign = StringUtils.split(param, ",")[1];
		
		String saveFilePath = settings.getProperty("edu_sign");
		File saveDir = new File(saveFilePath);
		
		String fileName = dataPk + "_sign.png";
		
		// 디렉토리 없으면 생성
		if (!saveDir.isDirectory()) {
			saveDir.mkdirs();
		}
		
		FileUtils.writeByteArrayToFile(new File(saveFilePath+fileName), Base64.decodeBase64(sign));
		
		// attachfile 삭제 및 저장 해야함
	}

	public List<Map<String, Object>> getPersonReadUser(Integer deptId) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("deptId", deptId);
        
        String sql = """
        		select up."User_id" as "userId" ,up."Name" as "userName", d.id as "deptId", d."Name" as "deptName" 
        		from user_profile up 
    			inner join depart d on up."Depart_id"  = d.id 
    			where d.id = :deptId
        		""";
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}
	
}
