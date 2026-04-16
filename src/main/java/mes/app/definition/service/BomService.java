package mes.app.definition.service;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.StringUtils;

import mes.domain.entity.Bom;
import mes.domain.entity.BomComponent;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BomComponentRepository;
import mes.domain.repository.BomRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.DateUtil;
import mes.domain.services.SqlRunner;

@Repository
public class BomService {

	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	BomRepository bomRepository;

	@Autowired
	BomComponentRepository bomComponentRepository;
	
	@Autowired
	TransactionTemplate transactionTemplate;	
	
	/**
	 * 
	 * @param mat_type
	 * @param mat_group
	 * @param bom_type
	 * @param mat_name
	 * @param not_past_flag
	 * @return
	 */
	public List<Map<String, Object>> getBomMaterialList(String mat_type, Integer mat_group,	String bom_type, String mat_name,String not_past_flag,String spjangcd){
		
        String sql = """        		
        		with A as (select b.id
				, b."Name"
				, b."BOMType"
				, fn_code_name('bom_type', b."BOMType") as bom_type_name
				, b."OutputAmount"
				, b."Version"
				, to_char(b."StartDate", 'yyyy-mm-dd') as "StartDate"
				, to_char(b."EndDate", 'yyyy-mm-dd') as "EndDate"
				, b."Material_id"
				, m."Name" as mat_name
				, m."Code" as mat_code
				, mg."Name" as mat_group_name
				, fn_code_name('mat_type', mg."MaterialType") as mat_type
				, u."Name" as unit
				, row_number() over (partition by b."BOMType", b."Material_id" order by b."StartDate" desc) as g_idx
				, case when to_char(current_date, 'yyyy-mm-dd') between to_char(b."StartDate", 'yyyy-mm-dd') and to_char(b."EndDate", 'yyyy-mm-dd') then 'current'
				    when b."StartDate" is null or b."EndDate" is null then 'error'
					when to_char(current_date, 'yyyy-mm-dd') > to_char(b."EndDate", 'yyyy-mm-dd') then 'past' 
					when to_char(current_date, 'yyyy-mm-dd') < to_char(b."StartDate", 'yyyy-mm-dd') then 'future'
					else 'error' end as current_flag
				from bom b 
				left join material m on b."Material_id" = m.id 
				left join unit u on u.id = m."Unit_id"
				left join mat_grp mg on mg.id=m."MaterialGroup_id" 
				where 1=1
				AND b.spjangcd = :spjangcd
                """;
            		
            if (StringUtils.hasText(mat_type)){            	
                sql+= """                		
                and mg."MaterialType" = :mat_type
                """;
            }

            if (mat_group!=null){            	
                sql+="""                		
                and m."MaterialGroup_id" = :mat_group
                """;
            }
            if (StringUtils.hasText(bom_type)){            	
                sql+="""            		
                and b."BOMType" = :bom_type
                """;
            }
            
             if(StringUtils.hasText( mat_name))            	 
                sql+=""" 
                and  (m."Code" like concat('%%',:mat_name,'%%') or m."Name" like concat('%%',:mat_name,'%%') )
                """;
            sql += """            		
            )
            select *
            from A
            """;
            if (not_past_flag.equals("Y")){
                sql += """
                where ( A.current_flag in ( 'current','future') or A.g_idx = 1 )
                """;
            }
            
            sql += """            		
            order by A.mat_group_name, A.mat_code , A.mat_name , A."Material_id", A.bom_type_name
            """;
            
            
            MapSqlParameterSource paramMap = new MapSqlParameterSource();
            
            paramMap.addValue("mat_type", mat_type);
            paramMap.addValue("mat_group", mat_group);
            paramMap.addValue("bom_type", bom_type);
            paramMap.addValue("mat_name", mat_name);
			paramMap.addValue("spjangcd", spjangcd);
            return this.sqlRunner.getRows(sql, paramMap);		
		
	}
	
	public Bom getBom(int id) {		
		return this.bomRepository.getBomById(id);		
	}
	
	
	/**
	 * 
	 * @param id
	 * @return
	 */
	public Map<String, Object> getBomDetail(int id){
	
		String sql = """				
	            select b.id
	            , b."Name"
	            , b."BOMType"
	            , b."OutputAmount"
	            , b."Version"
	            , to_char(b."StartDate", 'yyyy-mm-dd') as "StartDate"
	            , to_char(b."EndDate", 'yyyy-mm-dd') as "EndDate"
	            , b."Material_id"
	            , m."Name" as "MaterialName"
	            , m."Code" as mat_code
	            , mg."Name" as mat_group_name
	            , fn_code_name('mat_type', mg."MaterialType") as mat_type
	            from bom b 
	            left join material m on b."Material_id" = m.id 
	            left join mat_grp mg on mg.id = m."MaterialGroup_id"
	            where b.id=:id				
	        """;			
			
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
			paramMap.addValue("id", id);
	        return this.sqlRunner.getRow(sql, paramMap);		
	}
	
	/**
	 * 
	 * @param id
	 * @param materialId
	 * @param bomType
	 * @param version
	 * @return
	 */
	public boolean checkSameVersion(Integer id, Integer materialId, String bomType, String version) {
		boolean result = true;
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("Material_id", materialId);
		paramMap.addValue("BOMType", bomType);
		paramMap.addValue("Version", version);
		
		String sql ="select 1 from bom where \"Material_id\"=:Material_id and \"BOMType\"=:BOMType and \"Version\"=:Version";
		
		if (id!=null) {
			paramMap.addValue("id", id);
			sql+=" and id!=:id";			
		}
		
		List<Map<String, Object>> mapList = this.sqlRunner.getRows(sql, paramMap);
		if(mapList==null || mapList.size() == 0) {
			result = false;
		}
		
		return result;
	}
	
	/**
	 * 
	 * @param id
	 * @param materialId
	 * @param bomType
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public boolean checkDuplicatePeriod(Integer id, Integer materialId, String bomType, String startDate, String endDate) {
		
		boolean result = true;
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();		
		paramMap.addValue("materialId",materialId, java.sql.Types.INTEGER);
		paramMap.addValue("bomType",bomType);
		paramMap.addValue("startDate",startDate, java.sql.Types.TIMESTAMP);
		paramMap.addValue("endDate",endDate, java.sql.Types.TIMESTAMP);
		
		String sql = """
		select count(*) as cnt from bom where "Material_id" = :materialId and "BOMType" = :bomType  and "StartDate" <= :endDate and "EndDate" >= :startDate 				
		""";
		
		if (id!=null) {
			paramMap.addValue("bom_id", id);
			sql+=" and id <> :bom_id";
		}	
		List<Map<String, Object>> mapList = this.sqlRunner.getRows(sql, paramMap);
		
		if (mapList == null || (Long) mapList.get(0).get("cnt") == 0) {
			result = false;
		}
		
		return result;
	}
	
	public Bom saveBom(Bom bom){		
		return this.bomRepository.save(bom);
	}
	
	public BomComponent saveBomComponent(BomComponent bomComp) {
	    return this.bomComponentRepository.save(bomComp);
	}
	
	public BomComponent getBomComponent(int bcid) {		
		return this.bomComponentRepository.getBomComponentById(bcid);
	}	
	
	public Map<String, Object> getBomComponentDetail(int bcid){
		
		String sql = """
            select bc.id
              , bc."BOM_id"
              , fn_code_name('mat_type', mg."MaterialType") as mat_type
              , mg."Name" as group_name
              , m."Name" as "MaterialName"
              , m."Code" as mat_code
              , bc."Amount"
              , bc."Material_id"
              , m."Unit_id"
              , u."Name" as unit
              , bc."Description"
              , bc."_order"
              , bom."Name" as bom_name
              , pm."Name" as "ParentMaterialName"
              , bom."Material_id" as "ParentMaterial_id"
            from bom_comp bc
            inner join bom on bom.id=bc."BOM_id"
            left join material m on bc."Material_id"=m.id
            left join material pm on bom."Material_id"=pm.id
            left join unit u on u.id = m."Unit_id" 
            left join mat_grp mg on m."MaterialGroup_id" =mg.id
            where bc.id = :id				
		""";
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", bcid);
		
		return this.sqlRunner.getRow(sql, paramMap);		
	}

	public int deleteBomComponent(int bc_id) {
		int iRowEffected = 0;		
		String sql ="""
		delete from bom_comp where id=:bc_id				
		""";
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bc_id", bc_id);		
		iRowEffected=this.sqlRunner.execute(sql, paramMap);		
		return iRowEffected;
	}

	public List<Map<String, Object>> getBomComponentTreeList(int bomId){
		
		String sql = """
            with recursive bom_tree as 
            (
              with bom as (
                select b1.id as bom_pk, b1."Name", b1."Material_id" as prod_pk
                , nullif(b1."OutputAmount",0) as produced_qty
                , row_number() over(partition by b1."Material_id" order by b1."StartDate" desc) as g_idx
                from bom b1
                inner join bom b on b1."BOMType" = b."BOMType"
                where b.id = :id
              )
              select 1 as lvl
                , bc."Material_id"
                , bc._order::integer as item_order
                , bc."Material_id" as parent_mat_id
                , bc."Amount" as quantity
                , bom.produced_qty 
                , bc."Amount" / bom.produced_qty as bom_ratio
                , bc."Description"
                , 'base' as data_div
                , bc.id as bc_id
                , lpad(bc._order::text, 4, '0') as tot_order
                , bc."Material_id"::text as my_key
                , '' as parent_key
              from bom_comp bc
              inner join bom on bom.bom_pk = bc."BOM_id"
              where bc."BOM_id" = :id
              union all
              select bom_tree.lvl + 1 as lvl
                , bc."Material_id"
                , bc._order::integer as item_order
                , bom_tree."Material_id" as parent_mat_id
                , bc."Amount" as quantity
                --, (bom_tree.quantity * bc."Amount" / bom.produced_qty)::numeric(10,2) as produced_qty
                , bom.produced_qty 
                , bc."Amount" / bom.produced_qty * bom_tree.bom_ratio as bom_ratio
                , bc."Description"
                , 'child' as data_div
                , bc.id as bc_id
                , bom_tree.tot_order ||'-'||lpad(bc._order::text, 4, '0') as tot_order
                , bom_tree.my_key ||'-'||bc."Material_id"::text as my_key
                , bom_tree.my_key as parent_key
                from bom_tree 
                inner join bom on bom.prod_pk = bom_tree."Material_id"
                inner join bom_comp bc on bc."BOM_id" = bom.bom_pk
                where 1=1
                and bom.g_idx = 1
            )
            select bom_tree.lvl
                , bom_tree.my_key
              , case when bom_tree.data_div = 'child' then bom_tree.parent_key end as parent_key
              , bom_tree."Material_id" as mat_id
              , case when bom_tree.data_div = 'child' then bom_tree.parent_mat_id end as parent_mat_id
              , fn_code_name('mat_type', mg."MaterialType") as mat_type
              , m."Name" as mat_name
              , m."Code" as mat_code
              , bom_tree.quantity
              , bom_tree.produced_qty
              , bom_tree.bom_ratio::numeric(15,7)
              , concat(bom_tree.quantity::decimal,'/', bom_tree.produced_qty::decimal) as bom_qty
              , u."Name" as unit
              , bom_tree."Description"
              , bom_tree.bc_id
              , bom_tree.tot_order
            from bom_tree 
            inner join material m on m.id = bom_tree."Material_id"
            left join unit u on u.id = m."Unit_id" 
            left join mat_grp mg on m."MaterialGroup_id"=mg.id
            order by bom_tree.tot_order asc		
						
		""";
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();		
		paramMap.addValue("id", bomId);
		return this.sqlRunner.getRows(sql, paramMap);
	}
	
	public boolean checkDuplicateBomComponent(int bomId, Integer materialId) {
		boolean exist = false;
		
		String sql = """
		select count(*) from bom_comp where "Material_id"=:materialId and "BOM_id"=:bomId	
		""";
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("materialId", materialId);
		paramMap.addValue("bomId", bomId);
		
		int count = this.sqlRunner.queryForCount(sql, paramMap);
		exist = count==0?false:true;		
		return exist;
	}
	
	public AjaxResult bomReplicate(int bomId, User user) {
		
		Bom bom = this.bomRepository.getBomById(bomId);
		
		int materialId = bom.getMaterialId();
	    //new_bom.StartDate = '1900-01-01'
        //new_bom.EndDate = '1900-01-01'
		
		String sql = """
		select count(*) from bom where "Material_id"=:materialId and "StartDate"='1900-01-01' or "EndDate"='1900-01-01'
        """;
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("materialId", materialId);
		int count = this.sqlRunner.queryForCount(sql, paramMap);
		
		AjaxResult result = new AjaxResult();
		if (count>0) {			
            result.success = false;
            result.message = "복제된 BOM중 수정되지 않은 BOM이 \\n 존재하여 복제를 수행할 수 없습니다.";
            return result;
		}
		
		Float fVer=CommonUtil.tryFloat(bom.getVersion()) + (float)0.1;
		String newVer = fVer.toString();
		String newName = String.format("%s_Copy", bom.getName());		
		
		this.transactionTemplate.executeWithoutResult(status->{			

			try {
				Bom newBom = new Bom();			
				newBom.setName(newName);
				newBom.setVersion(newVer);
				newBom.setMaterialId(materialId);
				newBom.setBomType(bom.getBomType());
				newBom.setOutputAmount(bom.getOutputAmount());
				newBom.setStartDate(Timestamp.valueOf("1900-01-01 00:00:00"));
				newBom.setEndDate(Timestamp.valueOf("1900-01-01 00:00:00"));
				newBom.set_audit(user);
				newBom.setSpjangcd(bom.getSpjangcd());
				//신규BOM저장
				this.bomRepository.save(newBom);			
				
				//bom component 저장=>기존 component를 가져와서 저장
				String sqlInsert = """
		        insert into bom_comp("BOM_id", "Material_id" , "Amount" , _order , "Description" , "_created" , "_creater_id" )
			    select :new_pk as bom_pk, "Material_id" , "Amount" , _order , "Description" , now() , :user_pk
			    from bom_comp bc 
			    where "BOM_id" = :bom_pk				
				""";
				MapSqlParameterSource insertMap = new MapSqlParameterSource();
				insertMap.addValue("new_pk", newBom.getId());
				insertMap.addValue("user_pk", user.getId());
				insertMap.addValue("bom_pk", bomId);				
				result.data =sqlRunner.execute(sqlInsert, insertMap);
				
			}
			catch(Exception ex) {
				TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
				result.success=false;
				result.message = ex.toString();
			}			
		});
		
		
		return result;
	}
	
	/**
	 * 
	 * @param user
	 * @return
	 */
	public AjaxResult bomRevision(int bomId, User user) {
		
		AjaxResult result = new AjaxResult();
		Bom bom = this.bomRepository.getBomById(bomId);		
		bom.setEndDate(DateUtil.getYesterdayTimestamp());
		bom.set_audit(user);
		
		this.transactionTemplate.executeWithoutResult(status->{
			
			try {
				this.bomRepository.save(bom);
				
				Float fVer=CommonUtil.tryFloat(bom.getVersion()) + 1;
				String newVer = fVer.toString();
				String newName = String.format("%s V%s", bom.getName(), newVer);
				
				Bom newBom = new Bom();
				
				newBom.setName(newName);
				newBom.setMaterialId(bom.getMaterialId());
				newBom.setBomType(bom.getBomType());
				newBom.setOutputAmount(bom.getOutputAmount());
				newBom.setVersion(newVer);

				Timestamp start = DateUtil.getNowTimeStamp();
				newBom.setStartDate(start);
				newBom.setEndDate(Timestamp.valueOf("2100-12-31 29:59:59"));			
				newBom.set_audit(user);
				newBom.setSpjangcd(bom.getSpjangcd());
				
				this.bomRepository.save(newBom);
				
				//bom component 저장=>기존 component를 가져와서 저장
				String sql = """
		        insert into bom_comp("BOM_id", "Material_id" , "Amount" , _order , "Description" , "_created" , "_creater_id" )
			    select :new_pk as bom_pk, "Material_id" , "Amount" , _order , "Description" , now() , :user_pk
			    from bom_comp bc 
			    where "BOM_id" = :bom_pk				
				""";
				MapSqlParameterSource insertMap = new MapSqlParameterSource();
				insertMap.addValue("new_pk", newBom.getId());
				insertMap.addValue("user_pk", user.getId());
				insertMap.addValue("bom_pk", bomId);				
				result.data =sqlRunner.execute(sql, insertMap);
				
			}catch(Exception ex) {
				TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
				result.success=false;
				result.message = ex.toString();
			}				
		});
			
		
		return result;
	}

	public List<Map<String, Object>> getPriceByMatAndComp(int matPk, int company_id, String ApplyStartDate){
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("mat_pk", matPk);
		dicParam.addValue("company_id", company_id);
		dicParam.addValue("ApplyStartDate", ApplyStartDate);

		String sql = """
			select mcu.id 
            , mcu."Company_id"
            , c."Name" as "CompanyName"
            , mcu."UnitPrice" 
            , mcu."FormerUnitPrice" 
            , mcu."ApplyStartDate"::date 
            , mcu."ApplyEndDate"::date 
            , mcu."ChangeDate"::date 
            , mcu."ChangerName" 
            from mat_comp_uprice mcu 
            inner join company c on c.id = mcu."Company_id"
            where 1=1
            and mcu."Material_id" = :mat_pk
            and mcu."Company_id" = :company_id
            and to_date(:ApplyStartDate, 'YYYY-MM-DD') between mcu."ApplyStartDate"::date and mcu."ApplyEndDate"::date
            and mcu."Type" = '02'
            order by c."Name", mcu."ApplyStartDate" desc
        """;


		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}
}
