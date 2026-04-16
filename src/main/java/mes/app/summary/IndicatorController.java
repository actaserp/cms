package mes.app.summary;

import mes.domain.model.AjaxResult;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.DayOfWeek;
import java.time.YearMonth;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/summary/indicator")
public class IndicatorController {

	@Autowired
	SqlRunner sqlRunner;

	@GetMapping("/prod_read")
	public AjaxResult getProductionMonthList(
			@RequestParam(value = "cboYear", required = false) String cboYear,
			@RequestParam(value = "spjangcd") String spjangcd) {

		int year = Integer.parseInt(cboYear);
		Map<Integer, Integer> workdays = getWorkdaysPerMonth(year); // 월별 영업일수 계산

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("date_form", year + "-01-01");
		paramMap.addValue("date_to", year + "-12-31");
		paramMap.addValue("spjangcd", spjangcd);

		// 공통 SELECT (GUBN 1, 3용)
		String commonSelect = """
        SELECT
            jr."Material_id" AS mat_pk,
            fn_code_name('mat_type', mg."MaterialType") AS mat_type_name,
            mg."Name" AS mat_grp_name,
            m."Code" AS mat_code,
            m."Name" AS mat_name,
            u."Name" AS unit_name,
            m."Standard1" AS standard1
    """;

		// 생산수량 (GUBN = '1')
		StringBuilder sql = new StringBuilder();
		sql.append(commonSelect).append(", '1' AS gubn");
		for (int i = 1; i <= 12; i++) {
			sql.append(", SUM(CASE WHEN EXTRACT(MONTH FROM jr.\"ProductionDate\") = ")
					.append(i).append(" THEN jr.\"GoodQty\" ELSE 0 END) AS mon_").append(i);
		}
		sql.append("""
        FROM job_res jr
        INNER JOIN material m ON m.id = jr."Material_id"
        LEFT JOIN mat_grp mg ON mg.id = m."MaterialGroup_id"
        LEFT JOIN unit u ON u.id = m."Unit_id"
        WHERE jr."ProductionDate" BETWEEN CAST(:date_form AS DATE) AND CAST(:date_to AS DATE)
          AND jr."State" = 'finished'
          AND jr.spjangcd = :spjangcd
        GROUP BY jr."Material_id", mg."MaterialType", mg."Name", m."Code", m."Name", u."Name", m."Standard1"
    """);

		// 영업일수 (GUBN = '2') — jr 참조 안함!
		sql.append(" UNION ALL ");
		sql.append("""
        SELECT
            m.id AS mat_pk,
            fn_code_name('mat_type', mg."MaterialType") AS mat_type_name,
            mg."Name" AS mat_grp_name,
            m."Code" AS mat_code,
            m."Name" AS mat_name,
            u."Name" AS unit_name,
            m."Standard1" AS standard1,
            '2' AS gubn
    """);
		for (int i = 1; i <= 12; i++) {
			sql.append(", ").append(workdays.get(i)).append(" AS mon_").append(i);
		}
		sql.append("""
        FROM material m
        LEFT JOIN mat_grp mg ON mg.id = m."MaterialGroup_id"
        LEFT JOIN unit u ON u.id = m."Unit_id"
        WHERE EXISTS (
            SELECT 1 FROM job_res jr
            WHERE jr."Material_id" = m.id
              AND jr."ProductionDate" BETWEEN CAST(:date_form AS DATE) AND CAST(:date_to AS DATE)
              AND jr."State" = 'finished'
              AND jr.spjangcd = :spjangcd
        )
        GROUP BY m.id, mg."MaterialType", mg."Name", m."Code", m."Name", u."Name", m."Standard1"
    """);

		// 시간당 생산량 (GUBN = '3')
		sql.append(" UNION ALL ");
		sql.append(commonSelect).append(", '3' AS gubn");
		for (int i = 1; i <= 12; i++) {
			sql.append(", ROUND(")
					.append("CASE WHEN ").append(workdays.get(i)).append(" = 0 THEN 0 ELSE ")
					.append("SUM(CASE WHEN EXTRACT(MONTH FROM jr.\"ProductionDate\") = ").append(i)
					.append(" THEN jr.\"GoodQty\" ELSE 0 END) / ").append(workdays.get(i)).append(" / 8 END::numeric, 2) AS mon_")
					.append(i);
		}
		sql.append("""
        FROM job_res jr
        INNER JOIN material m ON m.id = jr."Material_id"
        LEFT JOIN mat_grp mg ON mg.id = m."MaterialGroup_id"
        LEFT JOIN unit u ON u.id = m."Unit_id"
        WHERE jr."ProductionDate" BETWEEN CAST(:date_form AS DATE) AND CAST(:date_to AS DATE)
          AND jr."State" = 'finished'
          AND jr.spjangcd = :spjangcd
        GROUP BY jr."Material_id", mg."MaterialType", mg."Name", m."Code", m."Name", u."Name", m."Standard1"
        ORDER BY mat_type_name, mat_grp_name, mat_name, mat_code, gubn
    """);

		List<Map<String, Object>> rows = sqlRunner.getRows(sql.toString(), paramMap);

		AjaxResult result = new AjaxResult();
		result.data = rows;
		return result;
	}

	private Map<Integer, Integer> getWorkdaysPerMonth(int year) {
		Map<Integer, Integer> workdays = new LinkedHashMap<>();
		for (int month = 1; month <= 12; month++) {
			YearMonth ym = YearMonth.of(year, month);
			int count = 0;
			for (int d = 1; d <= ym.lengthOfMonth(); d++) {
				DayOfWeek dow = ym.atDay(d).getDayOfWeek();
				if (dow != DayOfWeek.SATURDAY && dow != DayOfWeek.SUNDAY) {
					count++;
				}
			}
			workdays.put(month, count);
		}
		return workdays;
	}

	@GetMapping("/short_read")
	public AjaxResult getShortMonthList(
			@RequestParam(value = "cboYear", required = false) String cboYear,
			@RequestParam(value = "spjangcd") String spjangcd) {

		int year = Integer.parseInt(cboYear);
		Map<Integer, Integer> workdays = getWorkdaysPerMonth(year); // 월별 영업일수 계산

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("date_form", year + "-01-01");
		paramMap.addValue("date_to", year + "-12-31");
		paramMap.addValue("spjangcd", spjangcd);

		StringBuilder sql = new StringBuilder();
		sql.append("""
			WITH base_data AS (
				SELECT 
					s.id AS suju_id,
					s."Material_id" AS mat_pk,
					EXTRACT(MONTH FROM s."JumunDate") AS mon,
					(sh."ShipDate" - s."JumunDate") +1 AS wday,
					s."SujuQty",
					SUM(sp."Qty") OVER (PARTITION BY s.id) AS total_qty
				FROM suju s
				JOIN shipment sp ON s.id = sp."SourceDataPk"
				JOIN shipment_head sh ON sp."ShipmentHead_id" = sh.id
				WHERE s."JumunDate" BETWEEN CAST(:date_form AS DATE) AND CAST(:date_to AS DATE)
				  AND s."spjangcd" = :spjangcd
			),
			mat_data AS (
				SELECT 
					m.id AS mat_pk,
					fn_code_name('mat_type', mg."MaterialType") AS mat_type_name,
					mg."Name" AS mat_grp_name,
					m."Code" AS mat_code,
					m."Name" AS mat_name,
					u."Name" AS unit_name,
					m."Standard1" AS standard1
				FROM material m
				LEFT JOIN mat_grp mg ON mg.id = m."MaterialGroup_id"
				LEFT JOIN unit u ON u.id = m."Unit_id"
			)
			SELECT 
				b.mat_pk,
				m.mat_type_name,
				m.mat_grp_name,
				m.mat_code,
				m.mat_name,
				m.unit_name,
				m.standard1,
		""");

		for (int i = 1; i <= 12; i++) {
			sql.append("ROUND(AVG(CASE WHEN b.mon = ").append(i).append(" THEN b.wday ELSE NULL END)::numeric, 1) AS mon_").append(i);
			if (i < 12) sql.append(", ");
		}

		sql.append("""
			FROM base_data b
			JOIN mat_data m ON b.mat_pk = m.mat_pk
			WHERE b."SujuQty" <= b.total_qty
			GROUP BY b.mat_pk, m.mat_type_name, m.mat_grp_name, m.mat_code, m.mat_name, m.unit_name, m.standard1
			ORDER BY m.mat_name
		""");


		List<Map<String, Object>> rows = sqlRunner.getRows(sql.toString(), paramMap);

		AjaxResult result = new AjaxResult();
		result.data = rows;
		return result;
	}

}
