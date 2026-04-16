package mes.app.system.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class MenuSetupService {

	@Autowired
	SqlRunner sqlRunner;

	// 메뉴폴더 리스트 조회
	public List<Map<String, Object>> getFolderTreeList() {

		String sql = """
				SELECT mf.id
				     , mf."FolderName"
				     , mf."IconCSS"
				     , mf."_order"
				     , mf."FrontFolder_id"
				     , ff.folder_name AS "FrontFolderName"
				FROM menu_folder mf
				LEFT JOIN menu_front_folder ff ON ff.id = mf."FrontFolder_id"
				ORDER BY ff._order, mf."_order"
				""";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, null);

		return items;
	}

	// 메뉴항목 리스트 조회
	public List<Map<String, Object>> getMenuList(Integer folder_id) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("folder_id", folder_id);

		String sql = """
				select mi."MenuCode"
		        , mi."MenuName"
		        , mi."Url"
		        , mi."MenuFolder_id"
		        , mi._order
		        , mi.template
		        , mi."IconCSS"
		        , mf."FolderName"
		        from menu_item mi
		        inner join menu_folder mf on mf.id = mi."MenuFolder_id"
		        where mi."MenuFolder_id" = :folder_id
		        order by mi."_order"
			""";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

		return items;
	}

	// 소스메뉴 리스트 조회 (menu_item.template이 설정된 항목 기준)
	public List<Map<String, Object>> getGuiUseList(String unset, String keyword) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();

		StringBuilder sql = new StringBuilder("""
				SELECT mi."MenuCode",
				       COALESCE(mi."MenuName", mi."MenuCode") AS "MenuName",
				       mi."MenuFolder_id",
				       COALESCE(mf."FolderName", '') AS "FolderName",
				       mi.template,
				       mi."Url",
				       mi."_order",
				       mi."IconCSS",
				       CASE WHEN mi."MenuFolder_id" IS NOT NULL THEN true ELSE false END AS exists
				FROM menu_item mi
				LEFT JOIN menu_folder mf ON mf.id = mi."MenuFolder_id"
				WHERE mi.template IS NOT NULL
				""");

		if (!StringUtils.isEmpty(keyword)) {
			sql.append(" AND (mi.\"MenuCode\" = :keyword OR mi.\"MenuName\" = :keyword)");
			paramMap.addValue("keyword", keyword);
		}

		if (!StringUtils.isEmpty(unset)) {
			sql.append(" AND mi.\"MenuFolder_id\" IS NULL");
		}

		sql.append(" ORDER BY mi.\"MenuFolder_id\" , mi._order");

		return this.sqlRunner.getRows(sql.toString(), paramMap);
	}
}
