package mes.app.inventory.service;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class MaterialInoutService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getMaterialInout(String srchStartDt, String srchEndDt, String housePk,
			String matType, String matGrpPk, String keyword, String spjangcd) {
		
		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("srchStartDt", srchStartDt);
		param.addValue("srchEndDt", srchEndDt);
		param.addValue("housePk", housePk);
		param.addValue("matType", matType);
		param.addValue("matGrpPk", matGrpPk);
		param.addValue("keyword", keyword);
		param.addValue("spjangcd", spjangcd);
		
		String sql = """
					select distinct mi.id as mio_pk
                    , fn_code_name('inout_type', mi."InOut") as inout
                    , mi."Material_id"
                    , mi."InputType" 
                    , mi."OutputType" 
                    , case when mi."InOut" = 'in' then fn_code_name('input_type', mi."InputType") 
	                    when mi."InOut" = 'out' then fn_code_name('output_type', mi."OutputType") 
	                    when mi."InOut" = 'recall' then fn_code_name('recall_type', mi."OutputType")
	                    when mi."InOut" = 'return' then fn_code_name('return_type', mi."InputType")
	                    end as inout_type
                    , to_char(mi."InoutDate",'yyyy-mm-dd ') as "InoutDate"
                    , to_char(mi."InoutTime", 'hh24:mi') as "InoutTime"
                    , sh."Name" as "store_house_name"
                    , m."Code" as "material_code"
                    , m."Name" as "material_name"
                    , m."CurrentStock" 
                    , m."ValidDays"
                    , m."LotSize"
                    , m."PackingUnitQty"
                    , mi."StoreHouse_id"
                    , mih2."CurrentStock" as "HouseStock"
                    , m."SafetyStock" 
                    , coalesce(mi."InputQty", 0) as "InputQty"
                    , coalesce(mi."OutputQty", 0) as "OutputQty"
                    , u2."Name" as "unit_name"
                    , mi."Description" 
                    , fn_code_name('mat_type', mg."MaterialType") as material_type
                    --, coalesce(lot_cnt.lot_count,0) as lot_count
                    , (select count(ml."LotNumber") as lot_count 
                        from mat_lot ml 
                        where ml."SourceTableName" ='mat_inout' 
                        and ml."SourceDataPk" = mi.id
                        )  as lot_count 
                    , coalesce(mi."PotentialInputQty",0) as "potentialInputQty"
                    , fn_code_name('inout_state', mi."State" ) as "inout_state"
                    , var."StateName" as "state_name"
                    , tir."JudgeCode" as judge_code
                    , m."LotUseYN" as lot_use
                    from mat_inout mi 
                    inner join material m on mi."Material_id" = m.id
                    left join mat_grp mg on mg.id = m."MaterialGroup_id"
                    inner join store_house sh on mi."StoreHouse_id" = sh.id
                    left join unit u2 on m."Unit_id" = u2.id 
                    --left join mat_order mo on mi."MaterialOrder_id" = mo.id 
                    --and m.id = mo."Material_id" 
                    left join mat_in_house mih2 on mih2."Material_id"  = m.id
                    and mih2."StoreHouse_id" = mi."StoreHouse_id"
                    left join rela_data rd on mi.id = rd."DataPk2" and rd."RelationName" = 'mat_inout_test_result' and rd."TableName2"  = 'mat_inout'
                    left join bundle_head bh on bh.id = rd."DataPk1" and rd."RelationName" = 'mat_inout_test_result' and rd."TableName1"  = 'bundle_head'
                    left join v_appr_result var on var."SourceDataPk" = bh.id and var."SourceTableName" ='bundle_head'
                    left join test_result tr on tr."SourceDataPk"  = mi.id and tr."SourceTableName" = 'mat_inout'
                    left join test_item_result tir on tr.id = tir."TestResult_id"
                    where 1 = 1
                    and m."Useyn" = '0'
                    --and sh."HouseType" = 'material'
                    and mi."InoutDate" between cast(:srchStartDt as date) and cast(:srchEndDt as date)
                    and mi.spjangcd = :spjangcd
				""";
		
		if (StringUtils.isEmpty(housePk)==false) sql +=" and sh.id = cast(:housePk as Integer) ";
		if (StringUtils.isEmpty(matType)==false) sql +=" and mg.\"MaterialType\" = :matType ";
		if (StringUtils.isEmpty(matGrpPk)==false) sql +=" and m.\"MaterialGroup_id\" = cast(:matGrpPk as Integer) ";
		if (StringUtils.isEmpty(keyword)==false) sql +=" and upper(m.\"Name\") like concat('%%',upper(:keyword),'%%') ";
		
		sql += " order by \"InoutDate\" desc, \"InoutTime\" desc, mi.id desc ";
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, param);
        
        return items;
	}

	public List<Map<String, Object>> getMaterialInoutReceipt(String srchStartDt, String srchEndDt, String housePk,
													  String matType, String matGrpPk, String keyword, String spjangcd) {

		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("srchStartDt", srchStartDt);
		param.addValue("srchEndDt", srchEndDt);
		param.addValue("housePk", housePk);
		param.addValue("matType", matType);
		param.addValue("matGrpPk", matGrpPk);
		param.addValue("keyword", keyword);
		param.addValue("spjangcd", spjangcd);

		String sql = """
					select distinct mi.id as mio_pk
                    , fn_code_name('inout_type', mi."InOut") as inout
                    , mi."Material_id"
                    , mi."InputType" 
                    , mi."OutputType" 
                    , case when mi."InOut" = 'in' then fn_code_name('input_type', mi."InputType") 
	                    when mi."InOut" = 'return' then fn_code_name('return_type', mi."InputType")
	                    end as inout_type
                    , to_char(mi."InoutDate",'yyyy-mm-dd ') as "InoutDate"
                    , to_char(mi."InoutTime", 'hh24:mi') as "InoutTime"
                    , sh."Name" as "store_house_name"
                    , m."Code" as "material_code"
                    , m."Name" as "material_name"
                    , m."CurrentStock" 
                    , m."ValidDays"
                    , m."LotSize"
                    , m."PackingUnitQty"
                    , mi."StoreHouse_id"
                    , mih2."CurrentStock" as "HouseStock"
                    , m."SafetyStock" 
                    , coalesce(mi."InputQty", 0) as "InputQty"
                    , coalesce(mi."OutputQty", 0) as "OutputQty"
                    , u2."Name" as "unit_name"
                    , mi."Description" 
                    , fn_code_name('mat_type', mg."MaterialType") as material_type
                    --, coalesce(lot_cnt.lot_count,0) as lot_count
                    , (select count(ml."LotNumber") as lot_count 
                        from mat_lot ml 
                        where ml."SourceTableName" ='mat_inout' 
                        and ml."SourceDataPk" = mi.id
                        )  as lot_count 
                    , coalesce(mi."PotentialInputQty",0) as "potentialInputQty"
                    , fn_code_name('inout_state', mi."State" ) as "inout_state"
                    , var."StateName" as "state_name"
                    , tir."JudgeCode" as judge_code
                    , m."LotUseYN" as lot_use
                    from mat_inout mi 
                    inner join material m on mi."Material_id" = m.id
                    left join mat_grp mg on mg.id = m."MaterialGroup_id"
                    inner join store_house sh on mi."StoreHouse_id" = sh.id
                    left join unit u2 on m."Unit_id" = u2.id 
                    --left join mat_order mo on mi."MaterialOrder_id" = mo.id 
                    --and m.id = mo."Material_id" 
                    left join mat_in_house mih2 on mih2."Material_id"  = m.id
                    and mih2."StoreHouse_id" = mi."StoreHouse_id"
                    left join rela_data rd on mi.id = rd."DataPk2" and rd."RelationName" = 'mat_inout_test_result' and rd."TableName2"  = 'mat_inout'
                    left join bundle_head bh on bh.id = rd."DataPk1" and rd."RelationName" = 'mat_inout_test_result' and rd."TableName1"  = 'bundle_head'
                    left join v_appr_result var on var."SourceDataPk" = bh.id and var."SourceTableName" ='bundle_head'
                    left join test_result tr on tr."SourceDataPk"  = mi.id and tr."SourceTableName" = 'mat_inout'
                    left join test_item_result tir on tr.id = tir."TestResult_id"
                    where 1 = 1
                    and m."Useyn" = '0'
                    AND mi."InOut" IN ('in', 'return')
                    --and sh."HouseType" = 'material'
                    and mi."InoutDate" between cast(:srchStartDt as date) and cast(:srchEndDt as date)
                    and mi.spjangcd = :spjangcd
				""";

		if (StringUtils.isEmpty(housePk)==false) sql +=" and sh.id = cast(:housePk as Integer) ";
		if (StringUtils.isEmpty(matType)==false) sql +=" and mg.\"MaterialType\" = :matType ";
		if (StringUtils.isEmpty(matGrpPk)==false) sql +=" and m.\"MaterialGroup_id\" = cast(:matGrpPk as Integer) ";
		if (StringUtils.isEmpty(keyword)==false) sql +=" and upper(m.\"Name\") like concat('%%',upper(:keyword),'%%') ";

		sql += " order by \"InoutDate\" desc, \"InoutTime\" desc, mi.id desc ";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, param);

		return items;
	}

	public List<Map<String, Object>> getMaterialInoutIssue(String srchStartDt, String srchEndDt, String housePk,
															  String matType, String matGrpPk, String keyword, String spjangcd) {

		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("srchStartDt", srchStartDt);
		param.addValue("srchEndDt", srchEndDt);
		param.addValue("housePk", housePk);
		param.addValue("matType", matType);
		param.addValue("matGrpPk", matGrpPk);
		param.addValue("keyword", keyword);
		param.addValue("spjangcd", spjangcd);

		String sql = """
					select distinct mi.id as mio_pk
                    , fn_code_name('inout_type', mi."InOut") as inout
                    , mi."Material_id"
                    , mi."InputType" 
                    , mi."OutputType" 
                    , case when mi."InOut" = 'out' then fn_code_name('output_type', mi."OutputType") 
	                    when mi."InOut" = 'recall' then fn_code_name('recall_type', mi."OutputType")
	                    end as inout_type
                    , to_char(mi."InoutDate",'yyyy-mm-dd ') as "InoutDate"
                    , to_char(mi."InoutTime", 'hh24:mi') as "InoutTime"
                    , sh."Name" as "store_house_name"
                    , m."Code" as "material_code"
                    , m."Name" as "material_name"
                    , m."CurrentStock" 
                    , m."ValidDays"
                    , m."LotSize"
                    , m."PackingUnitQty"
                    , mi."StoreHouse_id"
                    , mih2."CurrentStock" as "HouseStock"
                    , m."SafetyStock" 
                    , coalesce(mi."InputQty", 0) as "InputQty"
                    , coalesce(mi."OutputQty", 0) as "OutputQty"
                    , u2."Name" as "unit_name"
                    , mi."Description" 
                    , fn_code_name('mat_type', mg."MaterialType") as material_type
                    --, coalesce(lot_cnt.lot_count,0) as lot_count
                    , (select count(ml."LotNumber") as lot_count 
                        from mat_lot ml 
                        where ml."SourceTableName" ='mat_inout' 
                        and ml."SourceDataPk" = mi.id
                        )  as lot_count 
                    , coalesce(mi."PotentialInputQty",0) as "potentialInputQty"
                    , fn_code_name('inout_state', mi."State" ) as "inout_state"
                    , var."StateName" as "state_name"
                    , tir."JudgeCode" as judge_code
                    , m."LotUseYN" as lot_use
                    from mat_inout mi 
                    inner join material m on mi."Material_id" = m.id
                    left join mat_grp mg on mg.id = m."MaterialGroup_id"
                    inner join store_house sh on mi."StoreHouse_id" = sh.id
                    left join unit u2 on m."Unit_id" = u2.id 
                    left join mat_in_house mih2 on mih2."Material_id"  = m.id
                    and mih2."StoreHouse_id" = mi."StoreHouse_id"
                    left join rela_data rd on mi.id = rd."DataPk2" and rd."RelationName" = 'mat_inout_test_result' and rd."TableName2"  = 'mat_inout'
                    left join bundle_head bh on bh.id = rd."DataPk1" and rd."RelationName" = 'mat_inout_test_result' and rd."TableName1"  = 'bundle_head'
                    left join v_appr_result var on var."SourceDataPk" = bh.id and var."SourceTableName" ='bundle_head'
                    left join test_result tr on tr."SourceDataPk"  = mi.id and tr."SourceTableName" = 'mat_inout'
                    left join test_item_result tir on tr.id = tir."TestResult_id"
                    where 1 = 1
                    and m."Useyn" = '0'
                    AND mi."InOut" IN ('out', 'recall')
                    and mi."OutputType" != 'disposal_out'
                    and mi."InoutDate" between cast(:srchStartDt as date) and cast(:srchEndDt as date)
                    and mi.spjangcd = :spjangcd
				""";

		if (StringUtils.isEmpty(housePk)==false) sql +=" and sh.id = cast(:housePk as Integer) ";
		if (StringUtils.isEmpty(matType)==false) sql +=" and mg.\"MaterialType\" = :matType ";
		if (StringUtils.isEmpty(matGrpPk)==false) sql +=" and m.\"MaterialGroup_id\" = cast(:matGrpPk as Integer) ";
		if (StringUtils.isEmpty(keyword)==false) sql +=" and upper(m.\"Name\") like concat('%%',upper(:keyword),'%%') ";

		sql += " order by \"InoutDate\" desc, \"InoutTime\" desc, mi.id desc ";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, param);

		return items;
	}

	public List<Map<String, Object>> getMaterialInoutDisposal(String srchStartDt, String srchEndDt, String housePk,
													  String matType, String matGrpPk, String keyword, String spjangcd) {

		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("srchStartDt", srchStartDt);
		param.addValue("srchEndDt", srchEndDt);
		param.addValue("housePk", housePk);
		param.addValue("matType", matType);
		param.addValue("matGrpPk", matGrpPk);
		param.addValue("keyword", keyword);
		param.addValue("spjangcd", spjangcd);

		String sql = """
					select distinct mi.id as mio_pk
                    , fn_code_name('inout_type', mi."InOut") as inout
                    , mi."Material_id"
                    , mi."InputType" 
                    , mi."OutputType" 
                    , case when mi."InOut" = 'in' then fn_code_name('input_type', mi."InputType") 
	                    when mi."InOut" = 'out' then fn_code_name('output_type', mi."OutputType") 
	                    when mi."InOut" = 'recall' then fn_code_name('recall_type', mi."OutputType")
	                    when mi."InOut" = 'return' then fn_code_name('return_type', mi."InputType")
	                    end as inout_type
                    , to_char(mi."InoutDate",'yyyy-mm-dd ') as "InoutDate"
                    , to_char(mi."InoutTime", 'hh24:mi') as "InoutTime"
                    , sh."Name" as "store_house_name"
                    , m."Code" as "material_code"
                    , m."Name" as "material_name"
                    , m."CurrentStock" 
                    , m."ValidDays"
                    , m."LotSize"
                    , m."PackingUnitQty"
                    , mi."StoreHouse_id"
                    , mih2."CurrentStock" as "HouseStock"
                    , m."SafetyStock" 
                    , coalesce(mi."InputQty", 0) as "InputQty"
                    , coalesce(mi."OutputQty", 0) as "OutputQty"
                    , u2."Name" as "unit_name"
                    , mi."Description" 
                    , fn_code_name('mat_type', mg."MaterialType") as material_type
                    --, coalesce(lot_cnt.lot_count,0) as lot_count
                    , (select count(ml."LotNumber") as lot_count 
                        from mat_lot ml 
                        where ml."SourceTableName" ='mat_inout' 
                        and ml."SourceDataPk" = mi.id
                        )  as lot_count 
                    , coalesce(mi."PotentialInputQty",0) as "potentialInputQty"
                    , fn_code_name('inout_state', mi."State" ) as "inout_state"
                    , var."StateName" as "state_name"
                    , tir."JudgeCode" as judge_code
                    , m."LotUseYN" as lot_use
                    from mat_inout mi 
                    inner join material m on mi."Material_id" = m.id
                    left join mat_grp mg on mg.id = m."MaterialGroup_id"
                    inner join store_house sh on mi."StoreHouse_id" = sh.id
                    left join unit u2 on m."Unit_id" = u2.id 
                    left join mat_in_house mih2 on mih2."Material_id"  = m.id
                    and mih2."StoreHouse_id" = mi."StoreHouse_id"
                    left join rela_data rd on mi.id = rd."DataPk2" and rd."RelationName" = 'mat_inout_test_result' and rd."TableName2"  = 'mat_inout'
                    left join bundle_head bh on bh.id = rd."DataPk1" and rd."RelationName" = 'mat_inout_test_result' and rd."TableName1"  = 'bundle_head'
                    left join v_appr_result var on var."SourceDataPk" = bh.id and var."SourceTableName" ='bundle_head'
                    left join test_result tr on tr."SourceDataPk"  = mi.id and tr."SourceTableName" = 'mat_inout'
                    left join test_item_result tir on tr.id = tir."TestResult_id"
                    where 1 = 1
                    and m."Useyn" = '0'
                    and mi."OutputType" = 'disposal_out'
                    and mi."InoutDate" between cast(:srchStartDt as date) and cast(:srchEndDt as date)
                    and mi.spjangcd = :spjangcd
				""";

		if (StringUtils.isEmpty(housePk)==false) sql +=" and sh.id = cast(:housePk as Integer) ";
		if (StringUtils.isEmpty(matType)==false) sql +=" and mg.\"MaterialType\" = :matType ";
		if (StringUtils.isEmpty(matGrpPk)==false) sql +=" and m.\"MaterialGroup_id\" = cast(:matGrpPk as Integer) ";
		if (StringUtils.isEmpty(keyword)==false) sql +=" and upper(m.\"Name\") like concat('%%',upper(:keyword),'%%') ";

		sql += " order by \"InoutDate\" desc, \"InoutTime\" desc, mi.id desc ";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, param);

		return items;
	}

	public List<Map<String, Object>> getMaterialInoutDetail(Integer mio_pk) {

		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("mio_pk", mio_pk);

		String sql = """
					select distinct mi.id as mio_pk
                    , fn_code_name('inout_type', mi."InOut") as inout
                    , mi."InOut" as "inoutSelect"
					, mg."Name" as "cboMaterialGroupName"
					, mg."id" as "cboMaterialGroup"
					, COALESCE(NULLIF(mi."InputType", ''), NULLIF(mi."OutputType", '')) AS "InoutType"
					, to_char(mi."InoutDate", 'yyyy-mm-dd') || 'T' || to_char(mi."InoutTime", 'hh24:mi') as "inoutDate"
					,COALESCE(
						   NULLIF(mi."InputQty", 0),
						   NULLIF(mi."OutputQty", 0),
						   NULLIF(mi."PotentialInputQty", 0),
						   0
						 ) AS "InoutQty"
					, mg."MaterialType" as "cboMaterialType"
                    , mi."Material_id"
                    , mi."InputType" 
                    , mi."OutputType" 
                    , case when mi."InOut" = 'in' then fn_code_name('input_type', mi."InputType") 
	                    when mi."InOut" = 'out' then fn_code_name('output_type', mi."OutputType") 
	                    when mi."InOut" = 'recall' then fn_code_name('recall_type', mi."OutputType")
	                    when mi."InOut" = 'return' then fn_code_name('return_type', mi."InputType")
	                    end as inout_type
                    , to_char(mi."InoutDate",'yyyy-mm-dd ') as "InoutDate"
                    , to_char(mi."InoutTime", 'hh24:mi') as "InoutTime"
                    , sh."Name" as "store_house_name"
                    , m."Code" as "Material_code"
                    , m."Name" as "Material_name"
                    , m."CurrentStock" 
                    , m."ValidDays"
                    , m."PackingUnitQty"
                    , mi."StoreHouse_id"
                    , mih2."CurrentStock" as "HouseStock"
                    , m."SafetyStock" 
                    , coalesce(mi."InputQty", 0) as "InputQty"
                    , coalesce(mi."OutputQty", 0) as "OutputQty"
                    , u2."Name" as "unit_name"
                    , mi."Description" 
                    , fn_code_name('mat_type', mg."MaterialType") as "cboMaterialTypeName"
                    , coalesce(mi."PotentialInputQty",0) as "potentialInputQty"
                    , fn_code_name('inout_state', mi."State" ) as "inout_state"
                    , var."StateName" as "state_name"
                    , tir."JudgeCode" as judge_code
                    , m."LotUseYN" as lot_use
                    , mi."Company_id" as "cboCompany"
                    , c."Name" as "CompanyName"
                    from mat_inout mi 
                    inner join material m on mi."Material_id" = m.id
                    left join mat_grp mg on mg.id = m."MaterialGroup_id"
                    inner join store_house sh on mi."StoreHouse_id" = sh.id
                    left join unit u2 on m."Unit_id" = u2.id 
                    left join mat_in_house mih2 on mih2."Material_id"  = m.id
                    and mih2."StoreHouse_id" = mi."StoreHouse_id"
                    left join rela_data rd on mi.id = rd."DataPk2" and rd."RelationName" = 'mat_inout_test_result' and rd."TableName2"  = 'mat_inout'
                    left join bundle_head bh on bh.id = rd."DataPk1" and rd."RelationName" = 'mat_inout_test_result' and rd."TableName1"  = 'bundle_head'
                    left join v_appr_result var on var."SourceDataPk" = bh.id and var."SourceTableName" ='bundle_head'
                    left join test_result tr on tr."SourceDataPk"  = mi.id and tr."SourceTableName" = 'mat_inout'
                    left join test_item_result tir on tr.id = tir."TestResult_id"
                    left join company c on c.id= mi."Company_id"
                    where 1 = 1
                    and m."Useyn" = '0'
					and mi.id = :mio_pk
				""";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, param);

		return items;
	}

	public List<Map<String, Object>> mioLotList(String mioId) {
		
		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("mioId", mioId);
		
		String sql = """
            select 
            mi.id as mio_id
            , ml.id as ml_id
            , ml."LotNumber" 
            , m."Name" as "MaterialName"
            , m."Code" as "MaterialCode" 
            , mg."Name" as "MaterialGroupName" 
            , m."MaterialGroup_id" 
            , m."Unit_id" 
            , m."ValidDays" 
            , u."Name" as "UnitName"
            , ml."InputQty"
            , m."Thickness"
            , m."Width"
            , m."Length"
            , to_char(ml."InputDateTime",'yyyy-MM-dd hh24:mi:ss') as "InputDateTime"
            , to_char(ml."EffectiveDate",'yyyy-MM-dd') as "EffectiveDate"
            , ml."Description"
            , ml."StoreHouse_id" as store_house_id
            from mat_lot ml  
                left join material m on m.id = ml."Material_id"
                left join mat_grp mg on mg.id = m."MaterialGroup_id" 
                left join unit u on u.id = m."Unit_id" 
                left join mat_inout mi on ml."SourceDataPk" = mi.id and ml."SourceTableName" ='mat_inout'
            where mi.id = cast(:mioId as Integer) 
			""";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, param);
		return items;
	}

	public List<Map<String, Object>> mioTestList(Integer mioId, Integer testResultId) {
		
		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("mioId", mioId);
		param.addValue("testResultId", testResultId);
		
		String sql = """
				select ti.id, up."Name" as "CheckName", ti."ResultType" as "resultType", to_char(tir."TestDateTime", 'YYYY-MM-DD') as "testDate"
				, tir."JudgeCode", tir."CharResult" , ti."Name" as name ,tir."Char1" as result1
				, tr.id as "testResultId", tr."TestMaster_id" as "testMasterId"
				from test_item_result tir
				inner join test_result tr on tr.id = tir."TestResult_id"
				inner join test_item ti on tir."TestItem_id"  = ti.id 
				inner join user_profile up on tir."_creater_id"  = up."User_id" 
				where tr."SourceTableName" = 'mat_inout' and tr."SourceDataPk" = :mioId
				and tr.id= :testResultId
				order by ti.id
				""";
		
		
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, param);
		
		return items;
	}

	public Integer getTestMasterByItem(Integer mioId) {
		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("mioId", mioId);

		String sql = """
                    SELECT tmm."TestMaster_id" AS testMasterId
                            FROM mat_inout mi
                            INNER JOIN test_mast_mat tmm ON mi."Material_id" = tmm."Material_id"
                            WHERE mi.id = :mioId
                            LIMIT 1
                """;

		List<Map<String, Object>> result = this.sqlRunner.getRows(sql, param);
		return result.isEmpty() ? null : (Integer) result.get(0).get("testMasterId");
	}

	public List<Map<String, Object>> prodTestListByTestMaster(Integer testMasterId) {
		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("testMasterId", testMasterId);

		String sql = """
                    SELECT tm.id AS testMasterId, ti.id, ti."Name" AS name, ti."ResultType" AS "resultType",
                           tim."SpecText" AS "specText", '' AS result1
                    FROM test_item_mast tim
                    INNER JOIN test_mast tm ON tim."TestMaster_id" = tm.id
                    INNER JOIN test_item ti ON tim."TestItem_id" = ti.id
                    WHERE tm.id = :testMasterId
                """;

		return this.sqlRunner.getRows(sql, param);
	}

	public List<Map<String, Object>> mioTestDefaultList() {
		
		String sql = """
				select ti.id,ti."Name" as name, ti."ResultType" as "resultType", '' as result1
				from test_item ti
				inner join test_method tm on ti."TestMethod_id"  = tm.id 
				where tm."Code"  = 'inout_test'
				order by ti.id
			    """;
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, null);
		
		return items;
	}

	public Map<String, Object> getEffectDate(Integer mioId) {
		
		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("mioId", mioId);
		
		String sql = """
				select (case when mi."EffectiveDate" = null then null else to_char(mi."EffectiveDate", 'YYYY-MM-DD') end)  as "EffectiveDate"
				from mat_inout mi 
				inner join material m on m.id = mi."Material_id"
				where mi.id = :mioId
				""";
		
		Map<String,Object> items = this.sqlRunner.getRow(sql, param);
		
		return items;
	}

	public List<Map<String, Object>> getBaljuList(Timestamp start, Timestamp end, String spjangcd) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("start", start);
		dicParam.addValue("end", end);
		dicParam.addValue("spjangcd", spjangcd);

		String sql = """
        select b.id
          , b."JumunNumber"
          , b."Material_id" as "Material_id"
          , mg."Name" as "MaterialGroupName"
          , mg.id as "MaterialGroup_id"
          , fn_code_name('mat_type', mg."MaterialType") as "MaterialTypeName"
          , m.id as "Material_id"
          , m."Code" as product_code
          , m."Name" as product_name
          , u."Name" as unit
          , b."SujuQty" as "SujuQty"
          , to_char(b."JumunDate", 'yyyy-mm-dd') as "JumunDate"
          , to_char(b."DueDate", 'yyyy-mm-dd') as "DueDate"
          , b."CompanyName"
          , b."Company_id"
          , b."SujuType"
          , fn_code_name('Balju_type', b."SujuType") as "BaljuTypeName"
          , to_char(b."ProductionPlanDate", 'yyyy-mm-dd') as production_plan_date
          , to_char(b."ShipmentPlanDate", 'yyyy-mm-dd') as shiment_plan_date
          , b."Description"
          , b."AvailableStock" as "AvailableStock"
          , b."ReservationStock" as "ReservationStock"
          , COALESCE(mi."SujuQty2", 0) AS "SujuQty2"
          , fn_code_name('balju_state', b."State") as "StateName"
          , fn_code_name('shipment_state', b."ShipmentState") as "ShipmentStateName"
          , b."State"
          , to_char(b."_created", 'yyyy-mm-dd') as create_date
          , case b."PlanTableName" when 'prod_week_term' then '주간계획' when 'bundle_head' then '임의계획' else b."PlanTableName" end as plan_state
          from balju b
          inner join material m on m.id = b."Material_id"
          inner join mat_grp mg on mg.id = m."MaterialGroup_id"
          left join unit u on m."Unit_id" = u.id
          left join company c on c.id= b."Company_id"
          LEFT JOIN (
			   SELECT
				   "SourceDataPk",
				   SUM("InputQty") AS "SujuQty2"
			   FROM mat_inout
			   WHERE "SourceTableName" = 'balju'
				 AND COALESCE("_status", 'a') = 'a'
				 AND "InOut" = 'in'
			   GROUP BY "SourceDataPk"
		   ) mi ON mi."SourceDataPk" = b.id
          where 1 = 1
          and b."JumunDate" between :start and :end 
          AND COALESCE(mi."SujuQty2", 0) < b."SujuQty"
          and b.spjangcd = :spjangcd
          and "State" != 'force_completion'
			order by b."JumunDate" desc,  m."Name"
			""";

//    log.info("발주 read SQL: {}", sql);
//    log.info("SQL Parameters: {}", dicParam.getValues());
		List<Map<String, Object>> itmes = this.sqlRunner.getRows(sql, dicParam);

		return itmes;
	}

	public List<Map<String, Object>> getBaljuInList(Timestamp start, Timestamp end, String spjangcd, Integer choComp, String keyword) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("start", start);
		dicParam.addValue("end", end);
		dicParam.addValue("spjangcd", spjangcd);
		dicParam.addValue("choComp", choComp);
		dicParam.addValue("keyword", keyword);

		String sql = """
        select b.id
          , b."JumunNumber"
          , b."Material_id" as "Material_id"
          , mg."Name" as "MaterialGroupName"
          , mg.id as "MaterialGroup_id"
          , fn_code_name('mat_type', mg."MaterialType") as "MaterialTypeName"
          , m.id as "Material_id"
          , m."Code" as product_code
          , m."Name" as product_name
          , u."Name" as unit
          , b."SujuQty" as "SujuQty"
          , to_char(b."JumunDate", 'yyyy-mm-dd') as "JumunDate"
          , to_char(b."DueDate", 'yyyy-mm-dd') as "DueDate"
          , b."CompanyName"
          , b."Company_id"
          , b."SujuType"
          , fn_code_name('Balju_type', b."SujuType") as "BaljuTypeName"
          , to_char(b."ProductionPlanDate", 'yyyy-mm-dd') as production_plan_date
          , to_char(b."ShipmentPlanDate", 'yyyy-mm-dd') as shiment_plan_date
          , b."Description"
          , b."AvailableStock" as "AvailableStock"
          , b."ReservationStock" as "ReservationStock"
          , COALESCE(mi."SujuQty2", 0) AS "SujuQty2"
          , COALESCE(mi_return."ReturnQty", 0) AS "ReturnQty"
          , fn_code_name('balju_state', b."State") as "StateName"
          , fn_code_name('shipment_state', b."ShipmentState") as "ShipmentStateName"
          , b."State"
          , to_char(b."_created", 'yyyy-mm-dd') as create_date
          , case b."PlanTableName" when 'prod_week_term' then '주간계획' when 'bundle_head' then '임의계획' else b."PlanTableName" end as plan_state
          from balju b
          inner join material m on m.id = b."Material_id"
          inner join mat_grp mg on mg.id = m."MaterialGroup_id"
          left join unit u on m."Unit_id" = u.id
          left join company c on c.id= b."Company_id"
          LEFT JOIN (
			   SELECT
				   "SourceDataPk",
				   SUM("InputQty") AS "SujuQty2"
			   FROM mat_inout
			   WHERE "SourceTableName" = 'balju'
				 AND COALESCE("_status", 'a') = 'a'
				 AND "InOut" = 'in'
			   GROUP BY "SourceDataPk"
		   ) mi ON mi."SourceDataPk" = b.id
		  LEFT JOIN (
			 SELECT
				 "SourceDataPk",
				 SUM("InputQty") AS "ReturnQty"
			 FROM mat_inout
			 WHERE "SourceTableName" = 'balju'
			   AND COALESCE("_status", 'a') = 'a'
			   AND "InOut" = 'return'
			 GROUP BY "SourceDataPk"
		 ) mi_return ON mi_return."SourceDataPk" = b.id
          where 1 = 1
          and b."JumunDate" between :start and :end 
          AND COALESCE(mi."SujuQty2", 0) > 0
          and b.spjangcd = :spjangcd
         """;

		if (StringUtils.isEmpty(keyword)==false) sql +=" and upper(m.\"Name\") like concat('%%',upper(:keyword),'%%') ";
		if(choComp != null) {
			sql += """ 
					and b."Company_id" = :choComp
					""";
		}

		sql += " order by b.\"JumunDate\" desc,  m.\"Name\" ";

//    log.info("발주 read SQL: {}", sql);
//    log.info("SQL Parameters: {}", dicParam.getValues());
		List<Map<String, Object>> itmes = this.sqlRunner.getRows(sql, dicParam);

		return itmes;
	}

}
