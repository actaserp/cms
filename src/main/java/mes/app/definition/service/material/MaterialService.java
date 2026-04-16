package mes.app.definition.service.material;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@Service
public class MaterialService {
	
	@Autowired
	SqlRunner sqlRunner;
	
	
	public List<Map<String, Object>> getMaterialList(String matType, String matGroupId, String keyword, String spjangcd, String useYnFlag){
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("mat_type", matType);
		paramMap.addValue("mat_group_id", matGroupId);
		paramMap.addValue("keyword", keyword);
		paramMap.addValue("spjangcd", spjangcd);
		paramMap.addValue("UseYn", useYnFlag);
        
        String sql = """
			select m.id
                --, mg."MaterialType" as mat_type
                , fn_code_name('mat_type', mg."MaterialType" ) as mat_type_name
                , mg."Name" as mat_grp_name
                , m."Code" as mat_code
                , m."Name" as mat_name
                , u."Name" as unit_name
                , f."Name" as factory_name
                , m."LotSize" as lot_size
                , m."CustomerBarcode" as customer_barcode
                , m."Thickness" as thickness
                , m."Width" as width
                , m."Length" as length
                , m."Height" as height
                , m."Weight" as weight
                , m."Color" as color
                , m."Usage" as usage
                , m."Class1" as class1
                , m."Class2" as class2
                , m."Class3" as class3
                , m."Standard1" as starndard1
                , m."Standard2" as standard2 
                , m."Description" as description
                , wc."Name" as workcenter_name
                , e."Name" as equip_name
                , concat(m."StandardTime",'(',m."StandardTimeUnit",')') as standard_time
                , m."InputManCount" as input_man_count 
                , m."InputManHour" as input_man_hour
                , m."VatExemptionYN" as vat_exempt_yn 
                , m."PurchaseOrderStandard" as purchase_order_standard
                , m."LeadTime" as leadtime
                , m."MinOrder" as min_order 
                , m."MaxOrder" as max_order
                , sh."Name" as storehouse_name
                , m."StoreHouseLoc" as storehouse_loc
                , m."ManagementLevel" as manage_level 
                , m."PackingUnitQty" as packing_unit_qty
                , m."PackingUnitName" as packing_unit_name
                , m."SafetyStock" as safety_stock
                , m."MaxStock" as max_stock
                , m."ProcessSafetyStock" as process_safety_stock
                , m."ValidDays" as valid_days
                , m."InTestYN" as intest_yn
                , m."OutTestYN" as outtest_yn 
                , r."Name" as routing_name
                , m."UnitPrice" as unit_price
                , coalesce(m."LotUseYN",'N') as "lotUseYn"
                , to_char(m._created, 'yyyy-mm-dd') as _created
                , m."Mtyn" as mtyn
                , m."Useyn" as useyn
                , m."Avrqty" as avrqty
                , CASE
					 WHEN b."Material_id" IS NOT NULL THEN 1
					 ELSE 0
				 END as "useBOM"
            from material m
            left join mat_grp mg on mg.id = m."MaterialGroup_id"
            left join unit u on u.id = m."Unit_id"
            left join factory f on f.id = m."Factory_id"
            left join store_house sh on sh.id = m."StoreHouse_id"
            left join work_center wc on wc.id = m."WorkCenter_id"
            left join equ e on e.id = m."Equipment_id"
            left join routing r on r.id = m."Routing_id"
            left join bom b
				on b."Material_id" = m.id
				and (b."StartDate" IS NULL OR b."StartDate" <= CURRENT_DATE)
				and (b."EndDate"   IS NULL OR b."EndDate"   >= CURRENT_DATE)
            where 1=1
            AND m.spjangcd = :spjangcd
            AND m."Useyn" = :UseYn
        """;
        if (StringUtils.isEmpty(matType)==false) sql +=" and mg.\"MaterialType\" = :mat_type ";
        if (StringUtils.isEmpty(matGroupId)==false) sql +=" and m.\"MaterialGroup_id\" = (:mat_group_id)::int ";
        if (StringUtils.isEmpty(keyword)==false) {
        	sql += """  
            		and ( m."Name" ilike concat('%',:keyword,'%')
            		or m."Code" ilike concat('%',:keyword,'%'))
    			""";
        }
        sql += "order by m.\"MaterialGroup_id\" , m.\"Name\" ";
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
		
	}
	
    public Map<String, Object> getMaterial(int matPK, String spjangcd){
    	MapSqlParameterSource paramMap = new MapSqlParameterSource();        
    	paramMap.addValue("mat_pk", matPK);
		paramMap.addValue("spjangcd", spjangcd);
        
        String sql = """
			select m.id, m."MaterialGroup_id" 
            , mg."Name" as mat_grp_name
            , mg."Code" as mat_grp_code
            , m."Code"
            , m."Name"
            , m."Unit_id"
            , m."Factory_id", m."WorkCenter_id"
            , m."Routing_id" 
            , m."StoreHouse_id"
            , m."StoreHouseLoc"
            , m."PackingUnitQty"
            , m."PackingUnitName"
            , m."SafetyStock" 
            , m."LeadTime", m."LotSize"
            , m."StandardTime", m."StandardTimeUnit" 
            , m."CustomerBarcode"
            , m."Thickness", m."Width", m."Length", m."Height", m."Weight"
            , m."Color"
            , m."Usage" 
            , m."Class1", m."Class2", m."Class3"
            , m."Standard1", m."Standard2" 
            , m."PurchaseOrderStandard", m."ManagementLevel"
            , m."Description"
            , m."InputManCount", m."InputManHour"
            , m."MaxStock"
            , m."CurrentStock", m."AvailableStock", m."ReservationStock"
            , m."ExpectedOutput"
            , m."VatExemptionYN"
            , m."Equipment_id"
            , m."MinOrder", m."MaxOrder" 
            , m."InTestYN", m."OutTestYN"
            , u."Name" as unit_name
            , m."UnitPrice"
            , m."ProcessSafetyStock" 
            , m."ValidDays" 
            , m."LotUseYN" as "lotUseYn"
            , m."Mtyn" as mtyn
            , m."Useyn" as useyn
            , m."Avrqty" as avrqty
            from material m
            left join mat_grp mg on m."MaterialGroup_id" = mg.id
            left join unit u on u.id = m."Unit_id"
            where m.id = :mat_pk
            AND m.spjangcd = :spjangcd
        """;
        	
        
        Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
        return item;
		
	}

	public int saveMaterial(MultiValueMap<String, Object> data) {
		Integer id = CommonUtil.tryIntNull(data.getFirst("id"));
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("id", id);
		dicParam.addValue("code", CommonUtil.tryString(data.getFirst("Code")));
		dicParam.addValue("name", CommonUtil.tryString(data.getFirst("Name")));
		dicParam.addValue("matGroupId", CommonUtil.tryIntNull(data.getFirst("MaterialGroup_id")));
		dicParam.addValue("unitId", CommonUtil.tryIntNull(data.getFirst("Unit_id")));
		dicParam.addValue("factoryId", CommonUtil.tryIntNull(data.getFirst("Factory_id")));
		dicParam.addValue("workcenterId", CommonUtil.tryIntNull(data.getFirst("WorkCenter_id")));
		dicParam.addValue("equipmentId", CommonUtil.tryIntNull(data.getFirst("Equipment_id")));
		dicParam.addValue("storeHouseId", CommonUtil.tryIntNull(data.getFirst("StoreHouse_id")));
		dicParam.addValue("storeHouseLoc", CommonUtil.tryString(data.getFirst("StoreHouseLoc")));
		dicParam.addValue("managementLevel", CommonUtil.tryString(data.getFirst("ManagementLevel")));
		dicParam.addValue("spjangcd", CommonUtil.tryString(data.getFirst("spjangcd")));

		dicParam.addValue("safetyStock", CommonUtil.tryFloatNull(data.getFirst("SafetyStock")));
		dicParam.addValue("maxStock", CommonUtil.tryFloatNull(data.getFirst("MaxStock")));
		dicParam.addValue("processSafetyStock", CommonUtil.tryFloatNull(data.getFirst("ProcessSafetyStock")));
		String validDaysStr = data.getFirst("ValidDays") != null ? data.getFirst("ValidDays").toString().trim() : "";

		
		if(data.containsKey("lot_use_yn")) {
			dicParam.addValue("lotUseYN", data.getFirst("lot_use_yn").toString());			
		} else {
			dicParam.addValue("lotUseYN", null);
		}

		if(data.containsKey("mtyn")) {
			dicParam.addValue("mtyn", data.getFirst("mtyn").toString());
		} else {
			dicParam.addValue("mtyn", 0);
		}

		if(data.containsKey("useyn")) {
			dicParam.addValue("useyn", data.getFirst("useyn").toString());
		} else {
			dicParam.addValue("useyn", 0);
		}

		dicParam.addValue("avrqty", CommonUtil.tryString(data.getFirst("avrqty")));
		dicParam.addValue("validDays", CommonUtil.tryString(data.getFirst("validDays")));
		dicParam.addValue("leadTime", CommonUtil.tryString(data.getFirst("leadTime")));
		dicParam.addValue("packingUnitQty", CommonUtil.tryFloatNull(data.getFirst("PackingUnitQty")));
		dicParam.addValue("packingUnitName", CommonUtil.tryString(data.getFirst("PackingUnitName")));
		dicParam.addValue("minOrder", CommonUtil.tryFloatNull(data.getFirst("MinOrder")));
		dicParam.addValue("maxOrder", CommonUtil.tryFloatNull(data.getFirst("MaxOrder")));
		dicParam.addValue("lotSize", CommonUtil.tryFloatNull(data.getFirst("LotSize")));

		dicParam.addValue("standardTime", CommonUtil.tryFloatNull(data.getFirst("StandardTime")));
		dicParam.addValue("standardTimeUnit", CommonUtil.tryString(data.getFirst("StandardTimeUnit")));
		dicParam.addValue("thickness", CommonUtil.tryFloatNull(data.getFirst("Thickness")));
		dicParam.addValue("width", CommonUtil.tryFloatNull(data.getFirst("Width")));
		dicParam.addValue("length", CommonUtil.tryFloatNull(data.getFirst("Length")));
		dicParam.addValue("height", CommonUtil.tryFloatNull(data.getFirst("Height")));
		dicParam.addValue("weight", CommonUtil.tryFloatNull(data.getFirst("Weight")));
		dicParam.addValue("color", CommonUtil.tryString(data.getFirst("Color")));
		dicParam.addValue("usage", CommonUtil.tryString(data.getFirst("Usage")));
		dicParam.addValue("class1", CommonUtil.tryString(data.getFirst("Class1")));
		
		dicParam.addValue("class2", CommonUtil.tryString(data.getFirst("Class2")));
		dicParam.addValue("class3", CommonUtil.tryString(data.getFirst("Class3")));
		dicParam.addValue("standard1", CommonUtil.tryString(data.getFirst("Standard1")));
		dicParam.addValue("standard2", CommonUtil.tryString(data.getFirst("Standard2")));
		dicParam.addValue("description", CommonUtil.tryString(data.getFirst("Description")));
		dicParam.addValue("customerBarcode", CommonUtil.tryString(data.getFirst("CustomerBarcode")));
		dicParam.addValue("inputManCount", CommonUtil.tryIntNull(data.getFirst("InputManCount")));
		dicParam.addValue("inputManHour", CommonUtil.tryFloatNull(data.getFirst("InputManHour")));
		dicParam.addValue("purchaseOrderStandard", CommonUtil.tryString(data.getFirst("PurchaseOrderStandard")));
		dicParam.addValue("vatExemptionYN", CommonUtil.tryString(data.getFirst("VatExemptionYN")));
		
		dicParam.addValue("routingId", CommonUtil.tryIntNull(data.getFirst("Routing_id")));
		dicParam.addValue("user_id", CommonUtil.tryIntNull(data.getFirst("user_id").toString()));
		
		String sql = "";
		
		if(id == null) {
			sql = """
					INSERT INTO public.material
						("_created"
						,"_creater_id"
						, "Code" 
						, "Name" 
						, "MaterialGroup_id" 
						, "Unit_id" 
						, "Factory_id" 
						, "WorkCenter_id" 
						, "Equipment_id" 
						, "StoreHouse_id" 
						, "StoreHouseLoc" 
						, "ManagementLevel" 
						, "SafetyStock" 
						, "MaxStock" 
						, "ProcessSafetyStock" 
						, "ValidDays"
						, "LotUseYN" 
						, "PackingUnitQty" 
						, "PackingUnitName" 
						, "MinOrder" 
						, "MaxOrder" 
						, "LotSize" 
						, "LeadTime"
						, "StandardTime" 
						, "StandardTimeUnit" 
						, "Thickness" 
						, "Width" 
						, "Length" 
						, "Height" 
						, "Weight" 
						, "Color" 
						, "Usage" 
						, "Class1" 
						,  "Class2" 
						, "Class3" 
						, "Standard1" 
						, "Standard2" 
						, "Description" 
						, "CustomerBarcode" 
						, "InputManCount" 
						, "InputManHour" 
						, "PurchaseOrderStandard" 
						, "VatExemptionYN" 
						, "Routing_id" 
						 , "Mtyn"
						 , "Useyn"
						 ,"Avrqty"
						 ,"spjangcd"
						 )
						VALUES
						(now()
						, :user_id
						, :code
						, :name
						, :matGroupId
						, :unitId
						, :factoryId
						, :workcenterId
						, :equipmentId
						, :storeHouseId
						, :storeHouseLoc
						, :managementLevel
						, :safetyStock
						, :maxStock
						, :processSafetyStock
						, :validDays
						, :lotUseYN
						, :packingUnitQty
						, :packingUnitName
						, :minOrder
						, :maxOrder
						, :lotSize
						, :leadTime
						, :standardTime
						, :standardTimeUnit
						, :thickness
						, :width
						, :length
						, :height
						, :weight
						, :color
						, :usage
						, :class1
						, :class2
						, :class3
						, :standard1
						, :standard2
						, :description
						, :customerBarcode
						, :inputManCount
						, :inputManHour
						, :purchaseOrderStandard
						, :vatExemptionYN
						, :routingId
						, :mtyn
						, :useyn
						, :avrqty
						, :spjangcd)
					""";
		}else {
			sql = """
					UPDATE public.material
					SET "_modified" = now()
					, "_modifier_id" = :user_id
					, "Code" = :code
					, "Name" = :name
					, "MaterialGroup_id" = :matGroupId
					, "Unit_id" = :unitId
					, "Factory_id" = :factoryId
					, "WorkCenter_id" = :workcenterId
					, "Equipment_id" = :equipmentId
					, "StoreHouse_id" = :storeHouseId
					, "StoreHouseLoc" = :storeHouseLoc
					, "ManagementLevel" = :managementLevel 
					, "SafetyStock" = :safetyStock
					, "MaxStock" = :maxStock
					, "ProcessSafetyStock" = :processSafetyStock
					, "ValidDays" = :validDays
					, "LotUseYN" = :lotUseYN
					, "PackingUnitQty" = :packingUnitQty 
					, "PackingUnitName" = :packingUnitName
					, "MinOrder" = :minOrder
					, "MaxOrder" = :maxOrder
					, "LotSize" = :lotSize
					, "LeadTime" = :leadTime
					, "StandardTime" = :standardTime
					, "StandardTimeUnit" = :standardTimeUnit
					, "Thickness" = :thickness
					, "Width" = :width
					, "Length" = :length
					, "Height" = :height
					, "Weight" = :weight
					, "Color" = :color
					, "Usage" = :usage
					, "Class1" = :class1
					, "Class2" = :class2
					, "Class3" = :class3
					, "Standard1" = :standard1
					, "Standard2" = :standard2
					, "Description" = :description 
					, "CustomerBarcode" = :customerBarcode
					, "InputManCount" = :inputManCount
					, "InputManHour" = :inputManHour
					, "PurchaseOrderStandard" = :purchaseOrderStandard
					, "VatExemptionYN" = :vatExemptionYN 
					, "Routing_id" = :routingId
					, "Mtyn" = :mtyn
					, "Useyn" = :useyn
					,"Avrqty" = :avrqty
					WHERE id = :id
					AND spjangcd = :spjangcd
					""";
		}
		
		
		
		return this.sqlRunner.execute(sql, dicParam);
	}
	
	public int deleteMaterial(int matPK){
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("mat_pk", matPK);
        String sql = "";
        
//        //품목에 연결된 단가삭제
//        sql = " delete from mat_comp_uprice where \"Material_id\" = :mat_pk";
//        this.sqlRunner.execute(sql, dicParam);
        
        //품목 삭제
    	sql = " UPDATE material SET \"Useyn\" = '1' WHERE id = :mat_pk;";
    	return this.sqlRunner.execute(sql, dicParam);
	}

	public int deleteMaterials(List<Integer> matPKs) {
		int total = 0;
		for (int matPK : matPKs) {
			MapSqlParameterSource dicParam = new MapSqlParameterSource();
			dicParam.addValue("mat_pk", matPK);

//			// 1. 단가 삭제
//			String sql = "delete from mat_comp_uprice where \"Material_id\" = :mat_pk";
//			this.sqlRunner.execute(sql, dicParam);

			// 2. 품목 삭제
			String sql = " UPDATE material SET \"Useyn\" = '1' WHERE id = :mat_pk;";
			total += this.sqlRunner.execute(sql, dicParam);
		}
		return total;
	}
}
