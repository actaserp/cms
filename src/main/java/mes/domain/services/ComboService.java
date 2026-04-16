package mes.domain.services;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;


@Service
public class ComboService {


	@Autowired
	SqlRunner sqlRunner;
	
	Map<String, ComboDataFunction> _dicFunc_;

	public ComboService() {
		this._dicFunc_ = new HashMap<String, ComboDataFunction>();
		
		// 알파벳 순서대로
		this._dicFunc_.put("area", this.area);
		this._dicFunc_.put("bom_version", this.bom_version);
		this._dicFunc_.put("bank", this.bank);
		this._dicFunc_.put("bankid", this.bankid);

		this._dicFunc_.put("ccp_proc", this.ccp_proc);
		this._dicFunc_.put("check_master", this.check_master);
		this._dicFunc_.put("company", this.company);
		this._dicFunc_.put("consu_mat_type", this.consu_mat_type);
		this._dicFunc_.put("das_server", this.das_server);
		this._dicFunc_.put("das_config", this.das_config);
		this._dicFunc_.put("data_month", this.data_month);
		this._dicFunc_.put("data_year", this.data_year);
		this._dicFunc_.put("defect_type", this.defect_type);
		this._dicFunc_.put("depart", this.depart);
		//this._dicFunc_.put("device_type", this.device_type);
		this._dicFunc_.put("doc_form", this.doc_form);
		this._dicFunc_.put("equipment", this.equipment);
		this._dicFunc_.put("equipment_ccp", this.equipment_ccp);
		this._dicFunc_.put("equipment_gongmu", this.equipment_gongmu);
		this._dicFunc_.put("equipment_code", this.equipment_code);
		this._dicFunc_.put("equipment_group", this.equipment_group);
		this._dicFunc_.put("equipment_type", this.equipment_type);
		this._dicFunc_.put("factory", this.factory);
		this._dicFunc_.put("haccp_item", this.haccp_item);
		this._dicFunc_.put("haccp_process", this.haccp_process);
		this._dicFunc_.put("haccp_process_workcenter", this.haccp_process_workcenter);
		this._dicFunc_.put("haccp_task_group", this.haccp_task_group);
		this._dicFunc_.put("hmi_form", this.hmi_form);
		this._dicFunc_.put("master_data", this.master_data);
		this._dicFunc_.put("material", this.material);
		this._dicFunc_.put("material_group", this.material_group);
		this._dicFunc_.put("menu_code", this.menu_code);
		this._dicFunc_.put("menu_folder", this.menu_folder);
		this._dicFunc_.put("menu_item", this.menu_item);
		//this._dicFunc_.put("menu_template", this.menu_template);
		this._dicFunc_.put("mold_class", this.mold_class);
		this._dicFunc_.put("process", this.process);
		this._dicFunc_.put("prod_week_term", this.prod_week_term);
		this._dicFunc_.put("person", this.person);
		this._dicFunc_.put("routing", this.routing);
		this._dicFunc_.put("shift", this.shift);
		this._dicFunc_.put("issuediv", this.issuediv);
		this._dicFunc_.put("stop_cause", this.stop_cause); // 확인필요
		this._dicFunc_.put("store_house", this.store_house);
		this._dicFunc_.put("store_house_semi_material", this.store_house_semi_material);
		this._dicFunc_.put("system_code", this.system_code);
		this._dicFunc_.put("system_codetype", this.system_codetype);
		this._dicFunc_.put("tag", this.tag); 
		this._dicFunc_.put("trade", this.trade);
		this._dicFunc_.put("tag_group", this.tag_group);
		this._dicFunc_.put("test_item", this.test_item);
		this._dicFunc_.put("test_master", this.test_master);
		this._dicFunc_.put("test_master_group", this.test_master_group);
		this._dicFunc_.put("test_method", this.test_method);
		this._dicFunc_.put("unit", this.unit);
		this._dicFunc_.put("task_master", this.task_master);
		this._dicFunc_.put("user_code", this.user_code);		
		this._dicFunc_.put("user_code_id", this.user_code_id);
		this._dicFunc_.put("user_group", this.user_group);
		this._dicFunc_.put("user_profile", this.user_profile);
		this._dicFunc_.put("workcenter", this.workcenter);
		this._dicFunc_.put("workcd", this.workcd);
		this._dicFunc_.put("iotype", this.iotype);
	}

	public List<Map<String, Object>> getComboList(String comboType, String cond1, String cond2, String cond3){
		if(this._dicFunc_.containsKey(comboType)) {			
			return this._dicFunc_.get(comboType).getDataList(cond1, cond2, cond3);			
		}else {
			return new ArrayList<Map<String,Object>>();
		}
	}

	public ComboDataFunction area=(String cond1, String cond2, String cond3)-> {  
		String sql = "select id as value, \"Name\" as text from area where 1=1 ";
		if (StringUtils.hasText(cond1)) {
			sql+="and \"Factory_id\" = cast(:cond1 as Integer) ";
		}
		sql += " order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);	
	};

	ComboDataFunction bom_version=(String cond1, String cond2, String cond3) -> { //확인 
		String sql = "select id as value, \"Version\" as text from bom where 1=1";
		if (StringUtils.hasText(cond1)) {
			sql += "and \"Material_id\" = :cond1 ";
		}
		sql += " order by \"Version\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};

	ComboDataFunction bank = (String banknm, String bankcd, String bankid) -> { //확인
		String sql = "select bankpopcd as value, bankpopnm as text from tb_xbank where 1=1";
		/*if (StringUtils.hasText(cond1)) {
			sql += "and \"Material_id\" = :cond1 ";
		}*/
		sql += " order by \"bankid\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		/*dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);*/
		return this.sqlRunner.getRows(sql, dicParam);
	};

	ComboDataFunction bankid = (String banknm, String bankcd, String bankid) -> { //확인
		String sql = "select bankid as value, banknm as text from tb_xbank where 1=1";
		/*if (StringUtils.hasText(cond1)) {
			sql += "and \"Material_id\" = :cond1 ";
		}*/
		sql += " order by \"bankid\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		/*dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);*/
		return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction check_master=(String cond1, String cond2, String cond3)-> { //확인
		String sql = "select id as value, \"Name\" as text from check_mast where 1=1";
		if (StringUtils.hasText(cond1)) {
			sql += "and \"CheckClassCode\" = :cond1 ";
		}
		sql += " order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction company=(String cond1, String cond2, String cond3)-> { 
		String sql = "select id as value, \"Name\" as text from company where 1=1 and spjangcd =:spjangcd ";
		if (StringUtils.hasText(cond1)) { 
			//sql +="and \"CompanyType\" = :cond1 ";
			sql += " and \"CompanyType\" in (select unnest(string_to_array(:cond1, ',')))";
		}
		sql += " order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		String tenantId = TenantContext.get();
		dicParam.addValue("spjangcd", tenantId);
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction consu_mat_type=(String cond1, String cond2, String cond3)-> { 
		String sql = "select \"Code\" as Value , \"Value\" as text from sys_code where \"CodeType\" = 'mat_type' and \"Code\" != 'product' and spjangcd =:spjangcd order by \"_ordering\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		String tenantId = TenantContext.get();
		dicParam.addValue("spjangcd", tenantId);
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction das_server=(String cond1, String cond2, String cond3)-> { 
		String sql = "select id as Value, \"Name\" as text from das_server where 1=1 order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction das_config=(String cond1, String cond2, String cond3)-> { 
		String sql = "select id as Value, \"Name\" as text from das_config where 1=1 order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	//ComboDataFunction data_month=(String cond1, String cond2, String cond3) -> {
	//};
	ComboDataFunction data_month=(String cond1, String cond2, String cond3) -> {
		String sql = "select generate_series(01,12)::text as value,generate_series(01,12)::text as text ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction data_year=(String cond1, String cond2, String cond3) -> {
		String sql = """
				select to_char(current_date, 'YYYY') as value , to_char(current_date,'YYYY') as text  
				union all
				select to_char((current_date -  interval '1 year'), 'YYYY') as value , to_char(current_date-  interval '1 year','YYYY') as text  
				union all
				select to_char((current_date -  interval '2 year'), 'YYYY') as value , to_char(current_date-  interval '2 year','YYYY') as text  
				""";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction defect_type=(String cond1, String cond2, String cond3) -> {

		String tenantId = TenantContext.get();

		String sql = "select id as Value, \"Name\" as text from defect_type where spjangcd = :spjangcd order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("spjangcd", tenantId);
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction depart=(String cond1, String cond2, String cond3) -> { //성공? 데이터 없음
		String tenantId = TenantContext.get();
		String sql = "select id as Value, \"Name\" as text from depart where spjangcd = :spjangcd order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("spjangcd", tenantId);
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	//ComboDataFunction device_type=(String cond1, String cond2, String cond3)-> {
	//};
	
	ComboDataFunction doc_form=(String cond1, String cond2, String cond3)-> { // 성공
		String sql = "select id as Value, \"FormName\"as text, \"FormType\" as form_type from doc_form df where 1=1 ";
		if (StringUtils.hasText(cond1)) {
			sql += "and \"FormType\" = :cond1 ";
		}
		sql += " order by \"FormName\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction equipment=(String cond1, String cond2, String cond3)-> { 
		String sql = """
				select e.id as value, e."Name" as text from equ e
				inner join equ_grp eg on eg.id = e."EquipmentGroup_id"  where 1=1 
		""";
		if (StringUtils.hasText(cond1)) {
			sql +="and eg.\"EquipmentType\" = :cond1 ";		
		}
		if (StringUtils.hasText(cond2)) { 
			sql +="and e.\"EquipmentGroup_id\" = :cond2 ";
		}
		if (StringUtils.hasText(cond3)) {
			sql +="and e.\"WorkCenter_id\" = cast(:cond3 as Integer) ";
		}
		sql += " order by e.\"Name\" ";
				
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);		
	};	
	
	ComboDataFunction equipment_gongmu=(String cond1, String cond2, String cond3)-> { 
		String sql = """
				SELECT e.id AS value,
				       '[' || coalesce(g."Name", '-') || '] ' || e."Name" AS text,
				       e."Code" AS code,
				       e."Depart_id" AS dept_id
				FROM equ e
				LEFT JOIN equ_grp g ON e."EquipmentGroup_id" = g.id
				WHERE (g."EquipmentType" IS NULL OR g."EquipmentType" IN ('manufacturing', 'etc'))
				  AND coalesce(e._status, 'A') = 'A'
				ORDER BY coalesce(g."Name", '힣'), e."Name"
				""";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);		
	};

	ComboDataFunction equipment_code=(String cond1, String cond2, String cond3)-> {
		String sql = "select " +
					 "              rd.id as id" +
					 "              , rd.\"DataPk1\" as \"WorkCenter_id\"" +
					 "              , rd.\"DataPk2\" as \"value\"" +
					 "              , e.\"Name\" as \"text\"" +
					 "              , wc.\"Name\" as \"WorkCenterName\"" +
					 "              , e.\"Code\" as \"Code\"" +
					 "            from rela_data rd " +
					 "                left join work_center wc on wc.id = rd.\"DataPk1\"" +
					 "                left join equ e on e.id = rd.\"DataPk2\"" +
					 "            where rd.\"TableName1\"='work_center' and rd.\"TableName2\"='equ'";

		if (StringUtils.hasText(cond1)) {
			sql += " and rd.\"DataPk1\" = :cond1"; // 중복 방지
		}

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("cond1", Integer.parseInt(cond1));
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);
	};

	ComboDataFunction equipment_group=(String cond1, String cond2, String cond3)-> { 
		String sql = "select id as value,\"Name\" as text from equ_grp order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);		
	};
	
	ComboDataFunction equipment_type=(String cond1, String cond2, String cond3) -> { 
		String sql = "select id as Value, \"Value\" as text from sys_code where \"CodeType\" = 'equipment_type'order by \"Value\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction factory=(String cond1, String cond2, String cond3) -> {
		String tenantId = TenantContext.get();
		String sql = "select id as Value, \"Name\" as text from factory where spjangcd = :spjangcd order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("spjangcd", tenantId);
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);	
	};
	
	ComboDataFunction haccp_item=(String cond1, String cond2, String cond3)-> { 
		String sql = "select id as Value, \"Name\" as text from haccp_item where 1=1 order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction haccp_process=(String cond1, String cond2, String cond3) -> {
		
		String sql = """		
		select id as value, \"Name\" as text from haccp_proc where 1=1 
	    """;
		if (StringUtils.hasText(cond1)) {		
			sql+=" and \"ProcessKind\"=:cond1";
		}
		sql+=" order by \"Name\"";
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction haccp_process_workcenter=(String cond1, String cond2, String cond3)-> {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		
		String sql ="""
			select wc.id as value
	        , wc."Name" as text
	        from haccp_proc hp
	        inner join rela_data rd on rd."TableName1" ='haccp_proc' 
	        and rd."DataPk1" = hp.id 
	        and rd."TableName2" ='process'
	        inner join process p on p.id = rd."DataPk2" 
	        inner join work_center wc on wc."Process_id" = p.id
	        where 1=1
			""";
		if(StringUtils.hasText(cond1)) {			
			int haccpProcessId = CommonUtil.tryInt(cond1);			
	        dicParam.addValue("hp_id", haccpProcessId);
			sql += "and hp.id = :hp_id ";
		}
		sql += "order by wc.\"Name\" ";		

        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);		
		return this.sqlRunner.getRows(sql, dicParam);
	};
	
	//ComboDataFunction hmi_form = (String cond1, String cond2, String cond3)-> {	
	//};
	
	ComboDataFunction material =(String cond1, String cond2, String cond3)-> { 
		String sql ="select m.id as Value, m.\"Name\" as text from material m inner join mat_grp mg on mg.id = m.\"MaterialGroup_id\" where 1=1 ";
		if (StringUtils.hasText(cond1)) {
			sql+="and \"MaterialGroup_id\" = :cond1::int";
		}
		if (StringUtils.hasText(cond2)) {
			sql+="and \"StoreHouse_id\" = :cond2";
		}
		if (StringUtils.hasText(cond3)) {
			//sql +="and mg.\"MaterialType\" = :cond3";
			sql +=" and mg.\"MaterialType\" in (select unnest(string_to_array(:cond3, ',')))";
		}
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction material_group = (String cond1, String cond2, String cond3)-> { 
		String sql ="""
		select id as value,"Name" as text from mat_grp where spjangcd = :spjangcd
		""";
		if(StringUtils.hasText(cond1)) {
			sql +=" and \"MaterialType\" in (select unnest(string_to_array(:cond1,',')))";
			//sql +=" and \"MaterialType\" =:cond1 ";
		}
		if (StringUtils.hasText(cond2)) {
			sql +=" and \"Code\" in (select unnest(string_to_array(:cond2, ',')))";
		}
		if (StringUtils.hasText(cond3)) {
			sql +=" and \"Code\" not in (select unnest(stirng_to_array(:cond3, ',')))";
		}
		
		sql += " order by \"Name\" ";
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		String tenantId = TenantContext.get();
		dicParam.addValue("spjangcd", tenantId);
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);
	};	
	
	ComboDataFunction menu_code =(String cond1, String cond2, String cond3)-> { //확인
		String sql = "";
		if (StringUtils.hasText(cond1)) {
			sql += "select \"MenuCode\" as Value, \"MenuName\" as text from menu_item where 1=1 and \"MenuFolder_id\" = cast(:cond1 as Integer)";
		}
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction menu_folder=(String cond1, String cond2, String cond3)-> { 
		String sql = "select id as value, \"FolderName\" as text from menu_folder mf  where mf.id != 55 order by \"_order\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction menu_item=(String cond1, String cond2, String cond3)-> {
		String sql = """
			select mi."MenuCode" as value, 
			format('%s(%s)',mi."MenuName", mf."FolderName") as text 
			from menu_item mi inner join menu_folder mf on mf.id = mi."MenuFolder_id"
			where mi."MenuFolder_id" != 55
			order by mi."MenuFolder_id";  
			""";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);
	};
	
	//ComboDataFunction menu_template=(String cond1, String cond2, String cond3)-> {
	//};
	
	ComboDataFunction mold_class=(String cond1, String cond2, String cond3)-> { //성공 - 데이터 없음 
		String sql = "select id as Value, \"Name\" as text from mold_cls where 1=1 order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction process=(String cond1, String cond2, String cond3)-> { 
		String sql = "select id as Value, \"Name\" as text from process where 1=1 order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};

	ComboDataFunction prod_week_term=(String cond1, String cond2, String cond3)-> {
		String year = null;
		if (StringUtils.hasText(cond1)) {
			year = cond1;
		} else {
			year = DateUtil.getYear();
		}
		String sql ="""
				select id as Value, format('%s~%s', substring(\"StartDate\"::varchar, 6),substring(\"EndDate\"::varchar,6)) as text 
				from prod_week_term 
				where 1=1
				and "DataYear" = :year
				order by "WeekIndex"
				""";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("year", year);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction person=(String cond1, String cond2, String cond3)-> { 
		String sql = "select id as Value, \"Name\" as text from person where 1=1 order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction routing=(String cond1, String cond2, String cond3)-> { 
		String sql = "select id as Value, \"Name\" as text from routing where 1=1 order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction shift=(String cond1, String cond2, String cond3)-> { 
		String sql = "select \"Code\" as Value, \"Name\" as text from shift where 1=1 and spjangcd =:spjangcd order by \"Code\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		String tenantId = TenantContext.get();
		dicParam.addValue("spjangcd", tenantId);
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);
	};

	ComboDataFunction issuediv=(String cond1, String cond2, String cond3)-> {
		String sql = "select \"Code\" as Value, \"Value\" as text from sys_code where 1=1 and \"CodeType\" = 'issue_div' order by \"Code\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction stop_cause=(String cond1, String cond2, String cond3)-> { // 확인필요
		String sql = "";
		if (cond1 == "equipment") {
			sql += """
				select sc.id as value, sc."StopCauseName" as text
		        from stop_cause sc 
		        inner join proc_stop_cause psc on psc."StopCause_id" = sc.id 
		        inner join work_center wc on wc."Process_id" = psc."Process_id" 
		        inner join equ e on e."WorkCenter_id" = wc.id 
	            where 1=1
	            and sc.spjangcd = :spjangcd
				""";
			if (StringUtils.hasText(cond2)) {
				sql += " and e.id = :cond2 ";
			}
			sql += "order by 2";
		}
		if (cond1 == "workcenter") {
			sql += """
				select sc.id as value, sc."StopCauseName" as text
		        from stop_cause sc 
		        inner join proc_stop_cause psc on psc."StopCause_id" = sc.id 
		        inner join work_center wc on wc."Process_id" = psc."Process_id" 
	            where 1=1
	            and sc.spjangcd = :spjangcd
				""";
			if (StringUtils.hasText(cond2)) {
				sql += "and wc.id = :cond2";
			}
			sql += "order by 2";
		}
		if (cond1 == "process") {
			sql += """
				select sc.id as value, sc."StopCauseName" as text
		        from stop_cause sc 
		        inner join proc_stop_cause psc on psc."StopCause_id" = sc.id 
	            where 1=1 
	            and sc.spjangcd = :spjangcd
				""";
			if (StringUtils.hasText(cond2)) {
				sql += "and psc.Process_id = cond2";
			}
			sql += "order by 2";
		}
		else {
			sql += """
				select sc.id as value, sc."StopCauseName" as text
		        from stop_cause sc 
		        where 1 = 1
		        and sc.spjangcd = :spjangcd
	            order by 2
				""";
		}
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		String tenantId = TenantContext.get();
		dicParam.addValue("spjangcd", tenantId);
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction store_house=(String cond1, String cond2, String cond3)-> {
		String sql = "select id as value,\"Name\" as text from store_house where 1=1 ";
		if (StringUtils.hasText(cond1)) sql +="and \"HouseType\" in (select unnest(string_to_array(:cond1,',')))";
		sql += " order by \"Name\" ";
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);
	};	
	
	ComboDataFunction store_house_semi_material=(String cond1, String cond2, String cond3)-> {  
		String sql = "select id as value, \"Name\" as text  from store_house  where \"HouseType\" in ('semi', 'material')";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction system_code = (String cond1, String cond2, String cond3)-> { 
		
		String sql = """
		select "Code" as value ,"Value" as text from sys_code where 1=1 
        """;
		if (StringUtils.hasText(cond1)) {
			sql +=" and \"CodeType\" = :cond1 ";
			//sql +=" and \"CodeType\" in (select unnest(string_to_array(cond1, ','))::string) ";
		}
		if (StringUtils.hasText(cond2)) {
			if(cond2.indexOf(',')>0){
				sql +=" and \"Code\" in (select unnest(string_to_array(:cond2::text, ','))::text) ";
			}
			else {
				sql +="and \"Code\" = :cond2 ";	
			}
		}
		
		sql += " order by \"_ordering\" ";
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);	
	};
	
	ComboDataFunction system_codetype=(String cond1, String cond2, String cond3)-> { 
		String sql ="select distinct  \"CodeType\" as Value, \"CodeType\" as text from sys_code order by \"CodeType\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);
	};

	//ComboDataFunction tag=(String cond1, String cond2, String cond3)-> {
	//};
	
	ComboDataFunction tag_group=(String cond1, String cond2, String cond3)-> { 
		String sql = "select id as Value, \"Name\" as text from tag_grp where 1=1 order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction test_item=(String cond1, String cond2, String cond3)-> { // 확인 -> 데이터 없음 
		String sql = "select id as Value, \"Name\" as text from test_item where 1=1 order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);
	};

	ComboDataFunction test_master=(String cond1, String cond2, String cond3)-> { // 확인 -> 데이터 없음 
		String sql = """
	    select tm.id as Value, tm.\"Name\" as text from test_mast tm
		inner join test_mast_grp tmg on tmg.id = tm."TestMasterGroup_id" where 1=1 
		""";
		if (StringUtils.hasText(cond1)) { 
			sql +="and tm.\"TestMasterGroup_id\" = cast(:cond1 as Integer) ";
		}
		
		if (StringUtils.hasText(cond2)) {
			sql +="and tm.\"TestType\" = :cond2 ";
		}
		
		if (StringUtils.hasText(cond3)) {
			sql +="and tmg.\"TestClass\" = :cond3 ";
		}
		
		sql += " order by tm.\"Name\" ";
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);		
	};	
	
	ComboDataFunction test_master_group=(String cond1, String cond2, String cond3)-> { //확인 -> 데이터 없음
		String sql = "select id as Value, \"Name\" as text from test_mast_grp order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);		
	};	
	
	ComboDataFunction test_method = (String cond1, String cond2, String cond3)-> { //확인 -> 데이터 없음 
		//String sql = "select id as Value, \"Name\" as text where test_method where 1=1 order by \"Name\" ";
		String sql = "select id as Value, \"Name\" as text from test_method where 1=1 order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction unit = (String cond1, String cond2, String cond3)-> {		
		String sql = """
			select id as value,"Name" as text from unit order by "Name"
		""";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);		
	};
	
	ComboDataFunction user_code=(String cond1, String cond2, String cond3)-> {
		
		String sql = "select \"Code\" as value, \"Value\" as text from user_code where 1=1 ";
		if (StringUtils.hasText(cond1)) { 
			sql +="and \"Parent_id\" in (select id from user_code where \"Code\" = :cond1) ";
		}
		
		sql += " order by \"Value\" ";
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);		
	};	
	
	ComboDataFunction user_code_id=(String cond1, String cond2, String cond3)-> {
		String sql = "select id as Value, \"Value\" as text, \"Code\" as code from user_code where 1=1 order by \"Code\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction user_group=(String cond1, String cond2, String cond3)-> {



		String sql = """
		select id as value, "Name" as text from user_group where 1=1 and spjangcd = :spjangcd order by "Name"
		""";
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		String tenantId = TenantContext.get();
		dicParam.addValue("spjangcd", tenantId);
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);		
	};
	
	ComboDataFunction user_profile=(String cond1, String cond2, String cond3)-> {
		String sql = "select \"User_id\" as Value, \"Name\" as text from user_profile where 1=1 order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);
	};

	ComboDataFunction workcenter=(String cond1, String cond2, String cond3)-> { 
		String	sql = "select id as value,\"Name\" as text from work_center where 1=1 and spjangcd =:spjangcd ";
		if (StringUtils.hasText(cond1)) { 
			sql +="and \"Process_id\" in (select unnest(string_to_array(:cond1,','))::int)";
		}
		if (StringUtils.hasText(cond2)) {
			sql +="and \"Area_id\" = :cond2 ";
		}
		sql += " order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		String tenantId = TenantContext.get();
			dicParam.addValue("spjangcd", tenantId);
			dicParam.addValue("cond1", cond1);
			dicParam.addValue("cond2", cond2);
			dicParam.addValue("cond3", cond3);
			return this.sqlRunner.getRows(sql, dicParam);
	};

	ComboDataFunction iotype=(String cond1, String cond2, String cond3)-> {
		String	sql = "select id as Value, \"Value\" as text from sys_code where \"CodeType\" = 'deposit_type'order by \"Value\" ";
		/*if (StringUtils.hasText(cond1)) {
			sql +="and \"Process_id\" in (select unnest(string_to_array(:cond1,','))::int)";
		}
		if (StringUtils.hasText(cond2)) {
			sql +="and \"Area_id\" = :cond2 ";
		}*/
		//sql += " order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		/*
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		*/
		return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction master_data=(String cond1, String cond2, String cond3)-> {
		String sql = "select \"id\" as Value, \"Name\" as text from master_t where 1=1 ";
		if (StringUtils.hasText(cond1)) { 
			sql +="and \"MasterClass\" = :cond1 ";
		}
		if (StringUtils.hasText(cond2)) { 
			sql +="and \"Type\" = :cond2 ";
		}
		if (StringUtils.hasText(cond3)) { 
			sql +="and \"Type2\" = :cond3 ";
		}
		sql += " order by \"Name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction hmi_form=(String cond1, String cond2, String cond3)-> {
		String sql = "select \"id\" as Value, \"FormName\" as text, \"FormType\" as form_type  from doc_form where 1=1 ";
		if (StringUtils.hasText(cond1)) { 
			sql +="and \"FormType\" = :cond1 ";
		} else {
			sql +="and \"FormType\" in ('hmi_a','hmi_b') ";
		}
		sql += " order by \"id\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("cond1", cond1);
		return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction tag=(String cond1, String cond2, String cond3)-> {
		String sql = "select \"tag_code\" as Value, \"tag_name\" as text from tag where 1=1 ";
		if (StringUtils.hasText(cond1)) { 
			sql +="and \"Equipment_id\" = cast(:cond1 as Integer) ";
		}
		if (StringUtils.hasText(cond2)) { 
			sql +="and \"tag_group_id\" = cast(:cond2 as Integer) ";
		}
		if (StringUtils.hasText(cond3)) { 
			sql +="and \"DASConfig_id\" = cast(:cond3 as Integer) ";
		}
		sql += " order by \"tag_name\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);
	};

	ComboDataFunction trade=(String cond1, String cond2, String cond3)-> {
		String sql = "select \"trid\" as Value, ioflag, \"tradenm\" as text from tb_trade where 1=1 ";
		/*if (StringUtils.hasText(cond1)) {
			sql +="and \"Equipment_id\" = cast(:cond1 as Integer) ";
		}
		if (StringUtils.hasText(cond2)) {
			sql +="and \"tag_group_id\" = cast(:cond2 as Integer) ";
		}
		if (StringUtils.hasText(cond3)) {
			sql +="and \"DASConfig_id\" = cast(:cond3 as Integer) ";
		}*/
		sql += " order by \"trid\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("cond2", cond2);
		dicParam.addValue("cond3", cond3);
		return this.sqlRunner.getRows(sql, dicParam);
	};
	
	ComboDataFunction task_master=(String cond1, String cond2, String cond3)-> {  
		String sql = "select id as value, \"TaskName\" as text from task_master where 1=1 ";
		if (StringUtils.hasText(cond1)) {
			sql+="and \"GroupCode\" = :cond1 ";
		}
		sql += " order by \"TaskName\" ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        return this.sqlRunner.getRows(sql, dicParam);	
	};
	
	ComboDataFunction ccp_proc=(String cond1, String cond2, String cond3)-> {  
		String sql = "select id as value, \"Name\" as text from haccp_proc hp  where 1=1 ";
		if (StringUtils.hasText(cond1)) {
		}
		sql += " order by id ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        return this.sqlRunner.getRows(sql, dicParam);	
	};
	
	ComboDataFunction equipment_ccp=(String cond1, String cond2, String cond3)-> {  
		String sql = """
				select distinct e.id as value, e."Name" as text from haccp_diary_devi_detect hddd 
				left join haccp_test ht ON ht.id = hddd."HaccpTest_id"
				left join equ e on e.id=ht."Equipment_id"
				left join haccp_diary hd on hddd."HaccpDiary_id"  = hd.id 
				left join haccp_proc hp on hd."HaccpProcess_id" = hp.id
				where 1=1
				""";
		if (StringUtils.hasText(cond1)) {
			sql += " and hp.id = cast(:cond1 as Integer)  ";
		}
		sql += " order by e.id ";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        return this.sqlRunner.getRows(sql, dicParam);	
	};
	
	ComboDataFunction haccp_task_group=(String cond1, String cond2, String cond3)-> {  
		String sql = """
				select "Code" as Value , "Value" as text from sys_code sc where "CodeType" = :cond1
				""";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        return this.sqlRunner.getRows(sql, dicParam);	
	};
	/* 템플리트
	
	public ComboDataFunction template=(String cond1, String cond2, String cond3)-> {
		String sql = "";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("cond1", cond1);
        dicParam.addValue("cond2", cond2);
        dicParam.addValue("cond3", cond3);
        return this.sqlRunner.getRows(sql, dicParam);		
	};
	*/

	ComboDataFunction workcd=(String cond1, String cond2, String cond3)-> {
		String sql = """
				select "workcd" as Value , "worknm" as text from tb_pb210 where spjangcd = :spjangcd
				""";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		String tenantId = TenantContext.get();
		dicParam.addValue("cond1", cond1);
		dicParam.addValue("spjangcd", tenantId);
		return this.sqlRunner.getRows(sql, dicParam);
	};

	
}
