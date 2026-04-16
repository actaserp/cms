package mes.app.definition.service.material;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;
import org.thymeleaf.util.MapUtils;

import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@Service
public class UnitPriceService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getPriceListByMat(int matPk){
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("mat_pk", matPk);
        
        String sql = """
			with A as 
            (
            select mcu.id 
            , mcu."Company_id"
            , mcu."UnitPrice"
            , mcu."FormerUnitPrice"
            , mcu."ApplyStartDate"
            , mcu."ApplyEndDate"
            , mcu."ChangeDate"
            , mcu."ChangerName"
            , mcu."Material_id"
            , row_number() over (partition by mcu."Company_id" order by mcu."ApplyStartDate" desc) as g_idx
            , now() between mcu."ApplyStartDate" and mcu."ApplyEndDate" as current_check
            , now() < mcu."ApplyStartDate" as future_check
            from mat_comp_uprice mcu 
            where mcu."Material_id" = :mat_pk
            )
            select A.id
            , A."Company_id"
            , c."Name" as "CompanyName"
            , A."UnitPrice" 
            , A."FormerUnitPrice" 
            , A."ApplyStartDate"::date 
            , A."ApplyEndDate"::date 
            , A."ChangeDate"::date 
            , A."Material_id"
            , A."ChangerName" 
            from A 
            inner join company c on c.id = A."Company_id"
            where ( A.current_check = true or A.future_check = true or A.g_idx = 1)
            order by c."Name", A."ApplyStartDate"
        """;
        	
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
	}
	
	public List<Map<String, Object>> getPriceHistoryByMat(int matPk, int comPk){
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("mat_pk", matPk);
		dicParam.addValue("com_pk", comPk);
        
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
            , mcu."Type" as type
            from mat_comp_uprice mcu 
            inner join company c on c.id = mcu."Company_id"
            where 1=1
            and mcu."Material_id" = :mat_pk
            and mcu."Company_id" = :com_pk
            order by c."Name", mcu."ApplyStartDate" desc
        """;
        	
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
	}
	
	public Map<String, Object> getPriceDetail(int pricePk){
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("price_pk", pricePk);
        
        String sql = """
			select mcu.id as price_id
            , m."MaterialGroup_id"
            , mg."MaterialType"
            , m."UnitPrice" as "UnitPrice"
            , mcu."Material_id" 
            , mcu."Company_id" 
            , mcu."UnitPrice" as "UnitPrices"
            , mcu."PartPrices" as "partPrices"
            , mcu."ProcPrices" as "procPrices"
            , "FormerUnitPrice"
            , mcu."ApplyStartDate" as "ApplyStartDate"
            , mcu."ApplyEndDate" as "ApplyEndDate"
            , mcu."Type" as type
            from mat_comp_uprice mcu 
            inner join material m on m.id = mcu."Material_id" 
            inner join mat_grp mg on m."MaterialGroup_id" = mg.id
            where 1 = 1
            and mcu.id = :price_pk
        """;
        	
        
        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
        return item;
	}

	public int saveCompanyUnitPrice(MultiValueMap<String, Object> data) {
		Integer materialId = CommonUtil.tryIntNull(data.getFirst("Material_id"));
		Integer companyId = CommonUtil.tryIntNull(data.getFirst("Company_id"));

		/*// applyStartDate가 '2025-04-15T13:34'와 같은 형식으로 들어올 때 처리
		String applyStartDateStr = CommonUtil.tryString(data.getFirst("ApplyStartDate"));
		LocalDateTime applyStartDateLocal = LocalDateTime.parse(applyStartDateStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
		Timestamp applyStartDate = Timestamp.valueOf(applyStartDateLocal);*/
		// ApplyStartDate 처리
		String applyStartDateStr = CommonUtil.tryString(data.getFirst("ApplyStartDate"));
		LocalDateTime applyStartDateLocal = LocalDateTime.parse(applyStartDateStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
		Timestamp applyStartDate = Timestamp.valueOf(applyStartDateLocal);

		// 현재 날짜와 비교하여 ApplyEndDate 설정
		LocalDate applyStartDateDate = applyStartDateLocal.toLocalDate();
		LocalDate today = LocalDate.now();

		Timestamp applyEndDate;
		if (!applyStartDateDate.equals(today)) {
			// 날짜가 다르면 하루 전 날짜로 설정 (시간은 00:00:00)
			applyEndDate = Timestamp.valueOf(applyStartDateDate.minusDays(1).atStartOfDay());
		} else {
			// 날짜가 같으면 ApplyStartDate 그대로 사용
			applyEndDate = applyStartDate;
		}

		// applyEndDate는 기존대로 설정
		Timestamp applyEndDate2 = CommonUtil.tryTimestamp("2100-12-31");


		Float unitPrice = CommonUtil.tryFloatNull(data.getFirst("UnitPrices")); // 납품단가
		Float partPrices = CommonUtil.tryFloatNull(data.getFirst("partPrices")); // 부품단가
		Float procPrices = CommonUtil.tryFloatNull(data.getFirst("procPrices")); // 가공비
		Float matUnitPrice = CommonUtil.tryFloatNull(data.getFirst("UnitPrice")); // 입고단가
		String changerName = CommonUtil.tryString(data.getFirst("ChangerName"));
		String type = CommonUtil.tryString(data.getFirst("type"));
		Integer userId = CommonUtil.tryIntNull(data.getFirst("user_id").toString());

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("materialId", materialId);
		dicParam.addValue("companyId", companyId);
		dicParam.addValue("applyStartDate", applyStartDate, java.sql.Types.TIMESTAMP);
		dicParam.addValue("applyEndDate", applyEndDate, java.sql.Types.TIMESTAMP);

		dicParam.addValue("applyEndDate2", applyEndDate2, java.sql.Types.TIMESTAMP);

		dicParam.addValue("unitPrice", unitPrice);
		dicParam.addValue("partPrices", partPrices);
		dicParam.addValue("procPrices", procPrices);
		dicParam.addValue("matUnitPrice", matUnitPrice);
		dicParam.addValue("changerName", changerName);
		dicParam.addValue("userId", userId);
		dicParam.addValue("type", type);
		dicParam.addValue("formerUnitPrice", null);

		String sql = """
            select id, "UnitPrice"
            from mat_comp_uprice
            where "Material_id" = :materialId
            and "Company_id" = :companyId
            and :applyStartDate between "ApplyStartDate" and "ApplyEndDate"
            """;

		Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

		if(!MapUtils.isEmpty(item)) {
			dicParam.addValue("formerUnitPrice", CommonUtil.tryFloatNull(item.get("UnitPrice")));
		}

		sql = """
        update mat_comp_uprice
        set "ApplyEndDate" = :applyEndDate
        where "Material_id" = :materialId
        and "Company_id" = :companyId
        and :applyStartDate between "ApplyStartDate" and "ApplyEndDate"
        """;

		this.sqlRunner.execute(sql, dicParam);

		sql = """
        update material
        set "UnitPrice" = :matUnitPrice
        where "id" = :materialId
        """;

		this.sqlRunner.execute(sql, dicParam);

		sql = """
            INSERT INTO public.mat_comp_uprice
            ("_created"
            , "_creater_id"
            , "Material_id"
            , "Company_id"
            , "ApplyStartDate"
            , "ApplyEndDate"
            , "UnitPrice"
            , "FormerUnitPrice"
            , "ChangeDate"
            , "ChangerName"
            , "Type"
            , "PartPrices"
            , "ProcPrices"
            )
            VALUES(
            now()
            , :userId
            , :materialId 
            , :companyId
            , :applyStartDate
            , :applyEndDate2
            , :unitPrice
            , :formerUnitPrice
            , now()
            , :changerName 
            , :type
            , :partPrices
            , :procPrices
            )
            """;
		return this.sqlRunner.execute(sql, dicParam);
	}
	
	public int updateCompanyUnitPrice(MultiValueMap<String, Object> data){
		Integer priceId = CommonUtil.tryIntNull(data.getFirst("price_id"));
		Timestamp applyStartDate = CommonUtil.tryTimestamp(data.getFirst("ApplyStartDate"));
		Float unitPrice = CommonUtil.tryFloatNull(data.getFirst("UnitPrices"));
		Float partPrices = CommonUtil.tryFloatNull(data.getFirst("partPrices"));
		Float procPrices = CommonUtil.tryFloatNull(data.getFirst("procPrices"));
		Float matUnitPrice = CommonUtil.tryFloatNull(data.getFirst("UnitPrice"));
		String changerName = CommonUtil.tryString(data.getFirst("ChangerName"));
		Integer userId = CommonUtil.tryIntNull(data.getFirst("user_id").toString());
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("priceId", priceId);
		dicParam.addValue("applyStartDate", applyStartDate, java.sql.Types.TIMESTAMP);
		dicParam.addValue("unitPrice", unitPrice);
		dicParam.addValue("partPrices", partPrices);
		dicParam.addValue("procPrices", procPrices);
		dicParam.addValue("matUnitPrice", matUnitPrice);
		dicParam.addValue("changerName", changerName);
		dicParam.addValue("userId", userId);

		String sql = """
				UPDATE material m
				SET "UnitPrice" = :matUnitPrice
				WHERE m."id" = (
				    SELECT u."Material_id"
				    FROM mat_comp_uprice u
				    WHERE u."id" = :priceId
				);
        """;

		this.sqlRunner.execute(sql, dicParam);

		sql = """
			update mat_comp_uprice
			set "FormerUnitPrice" = "UnitPrice"
			, "UnitPrice" = :unitPrice
			, "PartPrices" = :unitPrice
			, "ProcPrices" = :unitPrice
			, "ApplyStartDate" = :applyStartDate
			, "ChangeDate" = now()
			, "ChangerName" = :changerName
			where id = :priceId
        """;


		return this.sqlRunner.execute(sql, dicParam);
	}
	
	public int deleteCompanyUnitPrice(int priceId){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
		dicParam.addValue("priceId", priceId);
        
        String sql = """
				select id, "Material_id", "Company_id", to_char("ApplyStartDate",'yyyy-mm-dd') as "ApplyStartDate"
	            from mat_comp_uprice
	            where id = :priceId
				""";
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
		
    	sql = " delete from mat_comp_uprice where id = :priceId";
    	this.sqlRunner.execute(sql, dicParam);
    	
    	dicParam.addValue("materialId", CommonUtil.tryIntNull(item.get("Material_id")));
    	dicParam.addValue("companyId", CommonUtil.tryIntNull(item.get("Company_id")));
    	dicParam.addValue("applyStartDate", CommonUtil.tryTimestamp(item.get("ApplyStartDate")), java.sql.Types.TIMESTAMP);

		sql = """
    			UPDATE material m
				SET "UnitPrice" = null
				WHERE m."id" = :materialId
    			""";

		this.sqlRunner.execute(sql, dicParam);
    	
    	sql = """
    			update mat_comp_uprice
	            set "ApplyEndDate" = '2100-12-31'
	            where "Material_id" = :materialId
	            and "Company_id" = :companyId
	            and "ApplyEndDate" = (:applyStartDate)::timestamp - interval '1 days'
    			""";
    	
    	return this.sqlRunner.execute(sql, dicParam);
	}
}
