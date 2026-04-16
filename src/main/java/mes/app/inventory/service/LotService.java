package mes.app.inventory.service;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.entity.Company;
import mes.domain.entity.JobRes;
import mes.domain.entity.MaterialProduce;
import mes.domain.entity.SeqMaker;
import mes.domain.entity.Shipment;
import mes.domain.entity.ShipmentHead;
import mes.domain.repository.CompanyRepository;
import mes.domain.repository.JobResRepository;
import mes.domain.repository.MatProduceRepository;
import mes.domain.repository.SeqMakerRepository;
import mes.domain.repository.ShipmentHeadRepository;
import mes.domain.repository.ShipmentRepository;
import mes.domain.services.SqlRunner;

@Service
public class LotService {

	@Autowired
	SeqMakerRepository seqMakerRepository;
	
	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	MatProduceRepository matProduceRepository;
	
	@Autowired
	JobResRepository jobResRepository;

	@Autowired
	ShipmentRepository shipmentRepository;

	@Autowired
	ShipmentHeadRepository shipmentHeadRepository;

	@Autowired
	CompanyRepository companyRepository;
		
		
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
	
	// Lot 번호 만들기
	public String make_lot_in_number() {
		
		// 현재 날,시간
		Timestamp today = new Timestamp(System.currentTimeMillis());
		
		// 현재 일자
		LocalDate date = LocalDate.now();
		DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
		
		List<SeqMaker> sm = this.seqMakerRepository.findByCodeAndBaseDate("LOT_IN",date.format(dateFormat));
		
		SeqMaker s = new SeqMaker();
		
		if (sm.size() > 0) {
			s = sm.get(0);
		} else {
			s.setCode("LOT_IN");
			s.setBaseDate(date.format(dateFormat));
			s.setCurrVal(0);
			s.set_modified(today);
		}
		s.setCurrVal(s.getCurrVal() + 1);
		this.seqMakerRepository.save(s);
		
		String lotNumber = "LI-" + date.format(dateFormat) + "-" +String.format("%04d", s.getCurrVal());
		
		return lotNumber;
	}
	
	// Lot 번호 만들기
	public String make_production_lot_in_number(String type) {
		
		// 현재 날,시간
		Timestamp today = new Timestamp(System.currentTimeMillis());
		
		// 현재 일자
		LocalDate date = LocalDate.now();
		DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd");
		
		List<SeqMaker> sm = this.seqMakerRepository.findByCodeAndBaseDate("PROD_LOT_IN",date.format(dateFormat));
		
		SeqMaker s = new SeqMaker();
		
		if (sm.size() > 0) {
			s = sm.get(0);
		} else {
			s.setCode("PROD_LOT_IN");
			s.setBaseDate(date.format(dateFormat));
			s.setCurrVal(0);
			s.set_modified(today);
		}
		s.setCurrVal(s.getCurrVal() + 1);
		this.seqMakerRepository.save(s);
		
		String lotNumber = type + "-" + date.format(dateFormat) + "-" +String.format("%04d", s.getCurrVal());
		
		return lotNumber;
	}
	
	public List<Map<String, Object>> lotDetail(String lotNumber) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("lotNumber", lotNumber);
		
		String sql = """
		        select
			      ml.id as ml_id
		        , ml."LotNumber"
		        , ml."InputQty" 
		        , ml."CurrentStock" 
		        , m."Name" as mat_name
		        , mg."Name" as mat_group_name
		        , mg."MaterialType" as mat_type
                , to_char(ml."InputDateTime", 'yyyy-mm-dd hh24:mi') as "InputDateTime" 
	            , to_char(ml."EffectiveDate" , 'yyyy-mm-dd hh24:mi') as "EffectiveDate"
	            , fn_code_name('mat_type', mg."MaterialType" ) as mat_type_name
                , u."Name" as unit_name
		        , ml."SourceTableName" 
		        , ml."SourceDataPk" 
		        from mat_lot ml
		        inner join material m on m.id = ml."Material_id"
		        left join mat_grp mg on mg.id = m."MaterialGroup_id" 
		        left join unit u on u.id = m."Unit_id" 
		        where ml."LotNumber" = :lotNumber
				""";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}

	public List<Map<String, Object>> getMaterialTracking(String lotNumber) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("lotNumber", lotNumber);
		
		String sql = """
with recursive T as (    
         with P as(
            select 
            jr.id as jr_id
            , jr."WorkOrderNumber" 
            , mp.id as mp_id
            , ''::text as p_lot_number
            , mp."LotNumber" as lot_number
            , jr."Material_id" as mat_pk
            , ml."EffectiveDate"
            from job_res jr
            inner join mat_produce mp on mp."JobResponse_id" = jr.id            
            inner join material m on m.id=jr."Material_id" 
            left join mat_grp mg on mg.id = m."MaterialGroup_id" 
            inner join mat_lot ml on ml."SourceDataPk" =mp.id and ml."SourceTableName" ='mat_produce'
            where ml."LotNumber" = :lotNumber
         ) 
         ,A as(
          select 
           jr.jr_id
          , jr."WorkOrderNumber" 
          , mp."LotNumber" p_lot_number
          , mc."Material_id" as mat_pk
          , mp.id as mp_id
          , mc.id as mc_id
          , mc."BomQty"
          , mc."ConsumedQty" 
          from p as jr
          left join mat_consu mc on mc."JobResponse_id" = jr.jr_id
          left join mat_produce mp on mp."JobResponse_id" =jr.jr_id
        ) , B as(
        select 
        A.*
        ,ml."LotNumber" as lot_number
        , ml."Material_id"  
        , ml."EffectiveDate"
        from mat_lot ml 
        inner join mat_lot_cons mlc on mlc."MaterialLot_id" =ml.id
        inner join A on A.mp_id = mlc."SourceDataPk" and mlc."SourceTableName" ='mat_produce' and ml."Material_id" =A.mat_pk  
        )        
        select 
        jr_id
        , "WorkOrderNumber" 
        ,p_lot_number
        , lot_number
        , null::integer as mp_id
        , null::integer as p_mat_pk        
        , mat_pk 
        , 1 as lvl 
        , "EffectiveDate"
        from P
        union all 
        select 
        B.jr_id
        , B."WorkOrderNumber" 
        , T.lot_number as p_lot_number
        , B.lot_number 
        , B.mp_id
        , T.mat_pk as p_mat_pk
        , B.mat_pk as mat_pk
        , t.lvl +1 as lvl
        , B."EffectiveDate"
        from T   
          inner join B on B.p_lot_number  = T.lot_number 
        )        
        select 
        jr_id 
        , "WorkOrderNumber" 
        , p_mat_pk
        , mat_pk
        , p_lot_number
        , m1."Name" as p_mat_name
        , lot_number
        , m2."Name" as mat_name
        , (
            select 
            sum(mlc."OutputQty")
            from mat_lot ml 
            inner join mat_lot_cons mlc on mlc."MaterialLot_id"=ml.id 
            where T.mat_pk=ml."Material_id" and T.mp_id=mlc."SourceDataPk" and mlc."SourceTableName"='mat_produce'
            group by ml."Material_id" 
            ) as lot_consume_qty
        , T.mp_id
        , T.lvl
        , u."Name" as unit_name
        from T
        left join material m1 on m1.id = T.p_mat_pk
        inner join material m2 on m2.id = T.mat_pk
        left join unit u on u.id = m2."Unit_id"
        order by lvl
				""";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}

	public List<Map<String, Object>> getProductTracking(String lotNumber) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("lotNumber", lotNumber);
		
		String sql = """
		  with recursive T as(
            select 
            null::text as p_lot_number
            , ml."LotNumber" as lot_number  
            , null::float as mp_id
            , null::text as wo
            , null::int as p_mat_pk
            , ml."Material_id" as l_mat_pk
            , 1 as lvl
            from mat_lot ml        
            where ml."LotNumber" = :lotNumber
            union all 
            select 
             ml."LotNumber" as p_lot_numbe
            ,mp."LotNumber" as lot_number
            ,mp.id as mp_id
            , jr."WorkOrderNumber" as wo
            , mp."Material_id" as p_mat_pk
            , ml."Material_id" as l_mat_pk
            , (t.lvl+1 ) as lvl
            from mat_lot ml 
            inner join mat_lot_cons mlc ON mlc."MaterialLot_id" =ml.id 
            left join mat_produce mp on mp.id = mlc."SourceDataPk" and mlc."SourceTableName" ='mat_produce'
            inner join T on T.lot_number = ml."LotNumber" 
            inner join job_res jr on jr.id=mp."JobResponse_id" 
        )
          select 
          concat(p_lot_number , lot_number) as id
          , p_lot_number 
          , lot_number
          , m2."Name" as p_mat_name
          , m."Name" as mat_name
          , p_mat_pk
          , l_mat_pk
          , wo      
          , lvl
          ,(select sum(mlc."OutputQty") from mat_lot ml 
            inner join mat_lot_cons mlc on mlc."MaterialLot_id"=ml.id
            where T.l_mat_pk=ml."Material_id" and T.mp_id=mlc."SourceDataPk" and mlc."SourceTableName"='mat_produce'
            group by ml."Material_id") as lot_consume_qty
          , u."Name" as unit_name
          from T
          left join material m on m.id = l_mat_pk
          left join material m2 on m2.id = p_mat_pk
          left join unit u on u.id=m."Unit_id"
          order by lvl
				""";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}

	public List<Map<String, Object>> getMaterialInoutTracking(String lotNumber) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("lotNumber", lotNumber);
		
		String sql = """
	  with recursive T as (
         with P as(
            select 
            mp.id as mp_id
            , ''::text as p_lot_number
            , mp."LotNumber" as lot_number
            , jr."Material_id" as mat_pk
            from job_res jr
            left join mat_produce mp on mp."JobResponse_id" = jr.id 
            inner join mat_lot ml on ml."LotNumber" =mp."LotNumber" 
            where mp."LotNumber" = :lotNumber
         ) 
         ,A as(
          select 
          mp."LotNumber" p_lot_number
          , mc."Material_id" as mat_pk
          , mp.id as mp_id
          from job_res jr
          left join mat_consu mc on mc."JobResponse_id" = jr.id
          left join mat_produce mp on mp."JobResponse_id" =jr.id
        ), B as(
        select 
        A.*
        ,ml."LotNumber" as lot_number
        , ml."Material_id"
        from mat_lot ml 
        inner join mat_lot_cons mlc on mlc."MaterialLot_id" =ml.id 
        inner join A on A.mp_id = mlc."SourceDataPk" and mlc."SourceTableName" ='mat_produce' and ml."Material_id" =A.mat_pk 
        )
        select  
        p_lot_number, lot_number, null::integer as mp_id, mat_pk  
        from P
        union all 
        select 
        T.lot_number as p_lot_number, B.lot_number, B.mp_id, B.mat_pk as mat_pk
        from T   
          inner join B on B.p_lot_number  = T.lot_number 
        ), LL as
        ( 
        select T.lot_number from T 
        inner join material m2 on m2.id = T.mat_pk 
        left join mat_grp mg on mg.id = m2."MaterialGroup_id" 
        group by T.lot_number
        )
        select 
        LL.lot_number
        , to_char(ml."EffectiveDate" ,'yyyy-MM-dd hh24:mi:ss') as "EffectiveDate"
        , to_char(mi."InoutDate",'yyyy-MM-dd') as "InoutDate"
        , mi."InoutTime"
        , mi."InputQty"
        , fn_code_name('input_type',mi."InputType")as input_type_name
        , c."Name" as company_name
        , m."Name" as mat_name
        from LL 
        left outer join mat_lot ml on ml."LotNumber" =ll.lot_number and ml."SourceTableName" ='mat_inout'
        left join material m on m.id = ml."Material_id" 
        left join mat_inout mi on mi.id = ml."SourceDataPk"
        left join company c on c.id = mi."Company_id"
        order by m."Name", lot_number
				""";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}
	
	public List<Map<String, Object>> getProductShipmentTracking(String lotNumber) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("lotNumber", lotNumber);
		String sql = """
		 with recursive T as(
            select 
            null::text as p_lot_number
            , ml."LotNumber" as lot_number  
            , null::float as mp_id
            , null::int as p_mat_pk
            , ml."Material_id" as l_mat_pk
            from mat_lot ml
            where ml."LotNumber"= :lotNumber
            union all 
            select 
             ml."LotNumber" as p_lot_numbe
            ,mp."LotNumber" as lot_number
            , mp.id as mp_id
            , mp."Material_id" as p_mat_pk
            , ml."Material_id" as l_mat_pk
            from mat_lot ml 
            inner join mat_lot_cons mlc ON mlc."MaterialLot_id" =ml.id 
            left join mat_produce mp on mp.id = mlc."SourceDataPk" and mlc."SourceTableName" ='mat_produce'
            inner join T on T.lot_number = ml."LotNumber" 
            inner join job_res jr on jr.id=mp."JobResponse_id" 
	        ), pp as ( select lot_number from T group by lot_number)
	        select 
	        pp.lot_number 
	        , m."Name" as mat_name
	        , sh."Company_id" 
	        , c."Name" as company_name
	        , sh."ShipDate" 
	        , s."Qty" 
	        , fn_code_name('shipment_state', sh."State" ) as shipment_state
	        from pp 
	        inner join mat_lot ml on ml."LotNumber" =lot_number
	        inner join material m on m.id = ml."Material_id" 
	        inner join mat_lot_cons mlc on mlc."MaterialLot_id"=ml.id and mlc."SourceTableName" ='shipment'
	        inner join shipment s on s.id=mlc."SourceDataPk" 
	        inner join shipment_head sh on sh.id = s."ShipmentHead_id" 
	        left join company c on c.id = sh."Company_id"
				""";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public List<Map<String, Object>> getMatLotList(String mat_type, Integer mat_group, Integer material, String lot_num, String date_from, String date_to, String cond) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("mat_type", mat_type);
		dicParam.addValue("mat_group", mat_group);
		dicParam.addValue("material", material);
		dicParam.addValue("lot_num", lot_num);

		if (StringUtils.isEmpty(date_from) == false && StringUtils.isEmpty(date_to) == false) {
			dicParam.addValue("date_from", Timestamp.valueOf(date_from + " 00:00:00"));
			dicParam.addValue("date_to", Timestamp.valueOf(date_to + " 23:59:59"));
		}
		
        String sql = """
        		select ml.id
                , to_char(ml."InputDateTime", 'yyyy-mm-dd hh24:mi') as prod_date
                , ml."LotNumber" as lot_num
                , fn_code_name('mat_type', mg."MaterialType" ) as mat_type
                , mg."Name" as mat_group
                , m."Code" as mat_code
                , m."Name" as mat_name
                , ml."InputQty" as input_qty
                , ml."OutQtySum" as out_qty
                , ml."CurrentStock" as current_stock
                , ml."Description" as description
                , ml."SourceDataPk" as source_id
                , ml."SourceTableName" as source_table
                from mat_lot ml 
                inner join material m on m.id = ml."Material_id"
                left join mat_grp mg on mg.id = m."MaterialGroup_id"
                where 1=1
            """;

        if (StringUtils.isEmpty(mat_type)==false) {
        	sql += " and mg.\"MaterialType\" = :mat_type ";
        }

        if (mat_group != null) {
        	sql += " and mg.id = :mat_group ";
        }

        if (material != null) {
        	sql += " and m.id = :material ";
        }

        if (StringUtils.isEmpty(lot_num) == false) {
        	sql += " and ml.\"LotNumber\" ilike concat('%%',:lot_num,'%%') ";
        }
        
        if (StringUtils.isEmpty(date_from) == false && StringUtils.isEmpty(date_to) == false) {
        	sql += " and ml.\"InputDateTime\" between :date_from and :date_to ";
        }

        if ("remain".equals(cond)) {
        	sql += " and ml.\"CurrentStock\" > 0 ";
        }    
        
        sql += " order by prod_date desc ";
        		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        for (int i = 0; i < items.size(); i++) {
        	
        	if ("mat_produce".equals((String) items.get(i).get("source_table"))) {
        		
        		Integer source_id = (Integer) items.get(i).get("source_id");
        		MaterialProduce mpList = this.matProduceRepository.getMatProduceById(source_id);
        		
        		if (mpList != null) {
        			
        			Integer jobres_id = mpList.getJobResponseId();
        			JobRes jr = this.jobResRepository.getJobResById(jobres_id);
        			
        			if (jr != null) {
        				
        				String work_order_num = jr.getWorkOrderNumber();
        				
        				if (work_order_num != null) {        					
        					items.get(i).put("reg_history", "생산 (작지번호: " + work_order_num + ")");
        				}
        			}
        		}        		
        	}
        }
    	
        return items;
	}
	
	// LOT 소비내역 조회
	public List<Map<String, Object>> getConsumedList(Integer matlot_id) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("matlot_id", matlot_id);
		
        String sql = """
        		select mlc.id 
                , ml."LotNumber" as lot_num
                , to_char(mlc."OutputDateTime", 'yyyy-mm-dd hh24:mi') as consumed_date
                , mlc."OutputQty" as consumed_qty
                , mlc."Description" as description
                , mlc."SourceDataPk" as source_id
                , mlc."SourceTableName" as source_table
                from mat_lot_cons mlc 
                inner join mat_lot ml on ml.id = mlc."MaterialLot_id"
                where mlc."MaterialLot_id" = :matlot_id
            """;
        		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        for (int i = 0; i < items.size(); i++) {
        	
        	if ("shipment".equals((String) items.get(i).get("source_table"))) {
        		
        		Integer source_id = (Integer) items.get(i).get("source_id");
        		Shipment spList = this.shipmentRepository.getShipmentById(source_id);
        		
        		if (spList != null) {        			
        			Integer shipment_head_id = spList.getShipmentHeadId();
        			
        			if (shipment_head_id != null) {        				
        				ShipmentHead sh =  this.shipmentHeadRepository.getShipmentHeadById(shipment_head_id);
        				
        				if (sh != null) {        					
        					Integer company_id = sh.getCompanyId();
        					
        					if (company_id != null) {        						
        						Company company = this.companyRepository.getCompanyById(company_id);
        						
        						if (company != null) {
        							String company_name = company.getName();
        							
        							items.get(i).put("consumed_history", "출하 (고객사: " + company_name + ")");
        						}        								
        					}
        				}
        			}
        		}
        	}
        }
        
        return items;
	}
		

}
