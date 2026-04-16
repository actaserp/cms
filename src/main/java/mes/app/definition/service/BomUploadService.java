package mes.app.definition.service;

import mes.domain.repository.BomRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class BomUploadService {

	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	BomRepository bomRepository;


	
	
	// 수주 업로드 내역 조회 
	public List<Map<String, Object>> getSujuUploadList(String date_kind, String start, String end, String spjangcd) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		// data_kind : 'sales', 'delivery'
		dicParam.addValue("date_kind", date_kind);
		dicParam.addValue("start", start);
		dicParam.addValue("end", end);
		dicParam.addValue("spjangcd", spjangcd);
		
		String sql = """
			select 
            sb.id
            ,case when sb._status = 'Excel' then '엑셀' else '수주' end as state
            ,sb."JumunNumber" as jumun_number
            ,sb."CompCode" as company_code
            ,sb."CompanyName" as company_name
            ,sb."ProductCode" as product_code
            ,sb."ProductName" as product_name
            ,u."Name" as unit
            ,sb."Quantity" as suju_qty
            ,sb."JumunDate" as jumun_date
            ,sb."DueDate" as due_date
            from suju_bulk sb
            inner join material m on m."Code" = sb."ProductCode"
            --inner join material m on m.id = sb.id
            inner join unit u on u.id = m."Unit_id"
            where 1 = 1
            and sb.spjangcd = :spjangcd
			""";
		
		if (date_kind.equals("sales")) {
			sql += """
				and sb."JumunDate" between :start and :end
                order by sb."JumunDate" desc
				""";
		} else {
			sql +="""
				and sb."DueDate" between :start and :end
                order by sb."DueDate" desc
				""";
		}
		
		List<Map<String, Object>> itmes = this.sqlRunner.getRows(sql, dicParam);
		
		return itmes;
	}

	// 엑셀파일 파싱
	public List<List<String>> excel_read(String filename) throws IOException {
		List<List<String>> all_rows = new ArrayList<>();
		FileInputStream file = new FileInputStream(filename);
		XSSFWorkbook wb = new XSSFWorkbook(file);
		XSSFSheet sheet = wb.getSheetAt(0);

		for (int i = 0; i <= sheet.getLastRowNum(); i++) {
			XSSFRow row = sheet.getRow(i);
			if (row == null) continue; // 완전 빈행 skip

			XSSFCell jumunNumCell = row.getCell(0);
			if (jumunNumCell == null || jumunNumCell.getCellType() == CellType.BLANK) {
				continue; // 빈행 skip, 전체 데이터는 계속
			}

			List<String> value_list = new ArrayList<>();
			for (int j = 0; j < row.getLastCellNum(); j++) {
				XSSFCell cell = row.getCell(j);
				if (cell != null) {
					if (cell.getCellType() == CellType.NUMERIC && DateUtil.isCellDateFormatted(cell)) {
						value_list.add(new SimpleDateFormat("yyyy-MM-dd").format(cell.getDateCellValue()).strip());
					} else if (cell.getCellType() == CellType.NUMERIC) {
						value_list.add(CommonUtil.tryString(cell.getNumericCellValue()).strip());
					} else {
						value_list.add(cell.getStringCellValue().strip());
					}
				} else {
					value_list.add(""); // 셀이 null이면 빈값으로 넣기(셀누락 방지)
				}
			}
			all_rows.add(value_list);
		}

		wb.close();
		return all_rows;
	}

}
