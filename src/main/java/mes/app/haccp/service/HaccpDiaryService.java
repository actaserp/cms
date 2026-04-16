package mes.app.haccp.service;

import java.sql.Date;
import java.sql.Time;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import mes.domain.entity.HaccpDiaryDeviationDetect;
import mes.domain.entity.HaccpItem;
import mes.domain.entity.HaccpItemResult;
import mes.domain.entity.HaccpTest;
import mes.domain.entity.User;
import mes.domain.repository.HaccpDiaryDeviationDetectRepository;
import mes.domain.repository.HaccpItemRepository;
import mes.domain.repository.HaccpItemResultRepository;
import mes.domain.repository.HaccpTestRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@Service
public class HaccpDiaryService {
	
	@Autowired
	SqlRunner sqlRunner;

	@Autowired
	HaccpTestRepository haccpTestRepository;

	@Autowired
	HaccpItemResultRepository haccpItemResultRepository;
	
	@Autowired
    HaccpItemRepository haccpItemRepository;
	
	@Autowired
	TransactionTemplate transactionTemplate;
	
	@Autowired
	HaccpDiaryDeviationDetectRepository haccpDiaryDeviationDetectRepository;

	
	public List<Map<String, Object>> getDiaryList(String startDate, String endDate, Integer hp_id, String taskCode) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("start_date", Date.valueOf(startDate));   
		paramMap.addValue("end_date", Date.valueOf(endDate));
		paramMap.addValue("hp_id", hp_id);
		paramMap.addValue("taskCode", taskCode);
		
		String sql = """
		select
		hd."DataDate"
		, tm."TaskName"
		, hd.id as hd_id
		, hp.id as hp_id
		, hp."Name" as process_name
		, hp."Code" as process_code
		, coalesce(ar."SearchYN", 'Y') as "SearchYN"
		, coalesce(ar."EditYN", 'Y') as "EditYN"
		, coalesce(ar."DeleteYN", 'Y') as "DeleteYN"
		,ar."LineNameState"
		, coalesce(ar."StateName", '작성') as "StateName"
		, ar."LineName"
		, e."Name" as equ_name
		, e.id as equ_id
		, wc."Name" as workcenter_name
		, wc.id as wc_id
		from haccp_diary hd
		inner join haccp_proc hp on hp.id = hd."HaccpProcess_id"
		left join equ e on e.id = hd."Equipment_id"
		left join work_center wc on wc.id=e."WorkCenter_id"
		left join v_appr_result ar on ar."SourceDataPk" =hd.id and ar."SourceTableName" ='haccp_diary'
		left join task_master tm on tm."Code"=:taskCode
		where 1=1
		and hd."DataDate" between :start_date and :end_date
		and hp.id = :hp_id
		""";


		sql+="""
		order by hd."DataDate" desc
		""";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;		
	}	
	
	public Map<String, Object> getDiaryDetail(int hd_id){
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("hd_id", hd_id);
		
		String sql = """
            select hd.id as hd_id
            , hd."WorkCenter_id"
            , wc."Name" as "WorkCenterName"
            , hd."HaccpProcess_id"as hp_id
            , hp."Name" as "HaccpProcessName" 
            , hp."Code" as "HaccpProcessCode" 
            , hp."MonitoringMethod"
            , hp."ActionMethod"
            , hp."TestCycle"
            , hp."Standard" 
            , to_char(hd."DataDate", 'YYYY-MM-DD') as "DataDate"
            , hd."WriterName"
            , hd."Description"
            , hd."OverText"
            , hd."ActionText"
            , hd."ActionUserName"
            , hd."ConfirmUserName"
            , to_char(hd."_created", 'YYYY-MM-DD HH24:MI:SS') as _created
            , e."Name" as "EquipmentName"
            , hd."Equipment_id" 
            from haccp_diary hd
            inner join haccp_proc hp on hp.id = hd."HaccpProcess_id" 
            left join work_center wc on wc.id = hd."WorkCenter_id"
            left join equ e on e.id = hd."Equipment_id"
            where hd.id = :hd_id
		""";
		Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);

		return item;
	}
	
	public List<Map<String, Object>> getHaccpTestResultItemTreeList(int hd_id){

		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("hd_id", hd_id);
		
		String sql = """
		with A as(
		select ht."HaccpDiary_id"
		, hd."HaccpProcess_id"
		, ht.id as ht_id
		, ht."DataType"
		, substring( ht."StartTime"::text,1, 5) as "StartTime"
		, ht."EndTime"
		, ht."Material_id"
		, m."Name" as "MaterialName"
		, m."Code" as mat_code
		, ht."Equipment_id"
		, e."Name" as equ_name
		, ht."Judge" , ht."TesterName" , ht."Description"
		, null::integer as hir_id, null::integer as item_id,null as item_name, null as unit_name
		, null::float as "NumResult", null::float as "LowSpec", null::float as "UpperSpec", null as "SpecText"
		, 0 as _order
		from  haccp_test ht 
		inner join haccp_diary hd  on ht."HaccpDiary_id" = hd.id
		left join material m on m.id = ht."Material_id"
		left join equ e on e.id= ht."Equipment_id"
		where  ht."HaccpDiary_id" = :hd_id
		), B as (
		select null::integer as "HaccpDiary_id"
		, null::integer as "HaccpProcess_id"
		, tt.ht_id
		, tt."DataType"
		, tt."StartTime"
		 ,tt."EndTime"
		, null::integer as "Material_id"
		, null as"MaterialName"
		, null::integer as "Equipment_id"
		, null::text as equ_name
		, null as "Judge", null as "TesterName", null as "Description"
		, hir.id as hir_id, hi.id as item_id, hi."Name" as item_name, u."Name" as unit_name
		, case when hir."NumResult" is null then hir."CharResult"
		when hir."NumResult" is not null then hir."NumResult"::text end as "NumResult"
		, hil."LowSpec" , hil."UpperSpec", hil."SpecText" 
			, hpi._order
		   from A tt
		  --  inner join haccp_test ht on tt."HaccpDiary_id" = ht."HaccpDiary_id" 
		--and ht.id = tt.ht_id
		inner join haccp_proc hp on hp.id = tt."HaccpProcess_id"
		inner join haccp_proc_item hpi on hpi."HaccpProcess_id" = hp.id
		inner join haccp_item hi on hi.id = hpi."HaccpItem_id"
		left join unit u on u.id = hi."Unit_id"
		left join haccp_item_result hir on hir."HaccpTest_id" = tt.ht_id
		and hir."HaccpItem_id"= hpi."HaccpItem_id"
		left join haccp_item_limit hil on hil."HaccpProcess_id" = hp.id  
		and tt."Material_id" = hil."Material_id" 
		and hil."HaccpItem_id" = hpi."HaccpItem_id" 
		)
		select 1 as t_lvl, "HaccpDiary_id", "HaccpProcess_id", ht_id, "DataType", "StartTime", "EndTime", "Material_id",mat_code, "MaterialName", "Equipment_id", equ_name
		, "Judge", "TesterName", "Description", hir_id, item_id, item_name, unit_name
		, "NumResult"::text, "LowSpec", "UpperSpec", "SpecText", _order
		from A
		union all
		select 2 as t_lvl, "HaccpDiary_id", "HaccpProcess_id", ht_id, "DataType", "StartTime", "EndTime", "Material_id",'' as mat_code, "MaterialName", "Equipment_id", equ_name
		, "Judge", "TesterName", "Description", hir_id, item_id, item_name, unit_name
		, "NumResult", "LowSpec", "UpperSpec", "SpecText", _order
		from B 
		order by "StartTime", ht_id, t_lvl, _order;
		""";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);		
		return items;
	}	

    public int getHaccpDiaryCountByDataDateAndHaccpProcess(String data_date, int hp_id) {
        int count = 0;
        Date dataDate = CommonUtil.trySqlDate(data_date);
        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("hp_id", hp_id);
        paramMap.addValue("data_date", dataDate);

        String sql = """
        select count(*) from haccp_diary hd where hd."DataDate" = :data_date and hd."HaccpProcess_id"=:hp_id
        """;
        count = this.sqlRunner.queryForCount(sql, paramMap);
        return count;
    }
    
    public List<Map<String, Object>> getHaccpDeviDetectList(int hd_id){
    	
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("hd_id", hd_id);
    	
    	String sql = """
		select  
		hddd.id as hddd_id
		, hddd."HaccpDiary_id" as hd_id
		, ht.id as ht_id
		, hi.id as hi_id
		, to_char(hddd."StartTime" , 'HH24:MI') as "StartTime"
		, to_char(hddd."EndTime", 'HH24:MI') as "EndTime"
		, to_char(hddd."HappenTime", 'HH24:MI') as "HappenTime"
		, hddd."HappenPlace" 
		, hddd."AbnormalDetail" 
		, hddd."ActionDetail" 
		, hddd."ActionCode"
		, hddd."ActorName"
		, hddd."Substance" as substance
		, hddd."Description" as description
		, ht."Material_id"
		, ht."Equipment_id" as equ_id
		, e."Name" as equ_name
		, m."Name" as "MaterialName"
		, m."Code" as mat_code
		, hddd."Quantity" 
		, hddd."Substance" 
		, up."Name" as writer
		from haccp_diary_devi_detect hddd
		left join haccp_test ht ON ht.id = hddd."HaccpTest_id"
		left join haccp_item hi on hi.id =hddd."HaccpItem_id" 
		left join material m on m.id = ht."Material_id"
		left join auth_user au on au.id = hddd."_creater_id" 
		left join user_profile up on up."User_id" =au.id
		left join equ e on e.id=ht."Equipment_id"
		where hddd."HaccpDiary_id" = :hd_id
		order by hddd."StartTime"
		""";

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        return items;
    }
    
    @Transactional
    public void saveHaccpTestItemResult(List<Map<String,Object>> testItems, User user) throws Exception {

		for(Map<String, Object> t : testItems) {
			Integer ht_id = (Integer)t.get("ht_id");
			if (ht_id == null) {
				throw new Exception("잘못된 HaccpTest id 입니다. ");
			}

			String strStartTime = String.valueOf(t.get("StartTime"))+":00";
			String judge = t.get("Judge")==null?null:String.valueOf(t.get("Judge"));
			String desc = t.get("Description")==null?null:String.valueOf(t.get("Description"));

			String TesterName = String.valueOf(t.get("TesterName"));
			String dataType = String.valueOf(t.get("DataType"));
			HaccpTest haccpTest = this.haccpTestRepository.getHaccpTestById(ht_id);
			Time startTime = Time.valueOf(strStartTime);
			haccpTest.setStartTime(startTime);
			haccpTest.setJudge(judge);
			haccpTest.setDescription(desc);
			haccpTest.setTesterName(TesterName);
			haccpTest.set_audit(user);
			this.haccpTestRepository.save(haccpTest);
			
			// 부적합 판정시 자동으로 데이터 등록
			if("X".equals(judge)) {
				
				// 이미 존재하는지 체크할 것
				Optional<HaccpDiaryDeviationDetect> optHddd =this.haccpDiaryDeviationDetectRepository.findByHaccpTestId(ht_id);
				if(optHddd.isEmpty()) {
					HaccpDiaryDeviationDetect hddd = new HaccpDiaryDeviationDetect();
					hddd.setHaccpDiaryId(haccpTest.getHaccpDiaryId());
					hddd.setHaccpTestId(haccpTest.getId());
					hddd.setMaterialId(haccpTest.getMaterialId());
					hddd.setStartTime(haccpTest.getStartTime());
					hddd.setHappenTime(haccpTest.getStartTime());
					hddd.set_audit(user);
					this.haccpDiaryDeviationDetectRepository.save(hddd);
				}
			}

			@SuppressWarnings("unchecked")
			List<Map<String,Object>> items = (List<Map<String,Object>>)t.get("items");
			for(Map<String, Object> item : items) {
				int item_id = (int)item.get("item_id");
				String strResult = String.valueOf(item.get("NumResult"));

				Optional<HaccpItemResult> optHir = this.haccpItemResultRepository.getByHaccpTestIdAndHaccpItemId(ht_id, item_id);
				HaccpItem haccpItem = this.haccpItemRepository.getHaccpItemById(item_id);

				HaccpItemResult haccpItemResult = null;
				if(optHir.isPresent()) {
					haccpItemResult = optHir.get();
				}else {
					haccpItemResult = new HaccpItemResult();
					haccpItemResult.setHaccpTestId(ht_id);
					haccpItemResult.setHaccpItemId(item_id);
				}
				
				if("N".equals(haccpItem.getResultType())) {
					Float numResult = CommonUtil.tryFloat(strResult);
					haccpItemResult.setNumResult(numResult);
					
				}else {
					haccpItemResult.setCharResult(strResult);
					haccpItemResult.setNumResult((float)0);
				}
				haccpItemResult.setDataDiv(dataType);
				haccpItemResult.set_audit(user);
				this.haccpItemResultRepository.save(haccpItemResult);
			}
		}
    }
    
    public Map<String, Object> getHaccpDeviDetectActionDetail(int hddd_id){
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("hddd_id", hddd_id);
		
		String sql = """
		select  
		hddd.id as hddd_id
		, hddd."HaccpDiary_id" as hd_id
		, to_char(hddd."StartTime" , 'HH24:MI') as "StartTime"
		, to_char(hddd."EndTime", 'HH24:MI') as "EndTime"
		, to_char(hddd."HappenTime", 'HH24:MI') as "HappenTime"
		, hddd."HappenPlace" 
		, hddd."AbnormalDetail" 
		, hddd."ActionDetail"
		, hddd."ActionCode" 
		, hddd."ActorName" 
		, hddd."Description" as description
		, hddd."Substance" 
		, ht."Material_id"
		, ht."Equipment_id"
		, m."Name" as "mat_name"
		, hddd."Quantity" 
		, hddd."Substance" 
		, up."Name" as ConfirmName
		from haccp_diary_devi_detect hddd
		left join haccp_test ht ON ht.id = hddd."HaccpTest_id"
		left join haccp_item hi on hi.id =hddd."HaccpItem_id" 
		left join material m on m.id = ht."Material_id"
		left join auth_user au on au.id = hddd."_creater_id" 
		left join user_profile up on up."User_id" =au.id
		where hddd.id = :hddd_id
		""";
		
        Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
        return item;
    	
    }

    public List<Map<String, Object>> getHaccpDeviDetectActionCodeList(String parentCode){
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("parentCode", parentCode);
    	String sql = """
        select 
        c.id
        , c."Code"
        , c."Value"
        , c."Description"
	    from user_code c
	    where exists ( select 1 from user_code where "Code" = :parentCode and "Parent_id" is null and c."Parent_id" = id )
	    order by _order
    	""";
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        return items;
    }
    
    public boolean deleteHaccpTestById(int ht_id) {
    	
    	boolean result = true;
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("ht_id", ht_id);

		String sql = """
				delete from haccp_diary_devi_detect where "HaccpTest_id"=:ht_id;
				delete from haccp_item_result where "HaccpTest_id"=:ht_id;
				delete from haccp_test where id=:ht_id;
		""";
		
		this.sqlRunner.execute(sql, paramMap);
		
    	
    	return result;
    	
    }
}
