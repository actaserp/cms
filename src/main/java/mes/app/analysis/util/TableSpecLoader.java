package mes.app.analysis.util;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

@Component
public class TableSpecLoader {

    public Map<String, List<Map<String, String>>> loadFromExcel(String classpathPath) {
        Map<String, List<Map<String, String>>> tableMap = new LinkedHashMap<>();

        try (InputStream is = getClass().getClassLoader().getResourceAsStream(classpathPath);
             Workbook workbook = new XSSFWorkbook(is)) {

            Sheet sheet = workbook.getSheetAt(0);
            String currentTableName = null;

            for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                Row row = sheet.getRow(i);
                if (row == null) continue;

                String category = getCellValue(row.getCell(0));
                String tableName = getCellValue(row.getCell(1));
                String columnName = getCellValue(row.getCell(2));
                String dataType = getCellValue(row.getCell(3));
                String description = getCellValue(row.getCell(4));

                if (tableName != null && !tableName.isBlank()) {
                    currentTableName = tableName;
                }

                if (currentTableName == null || columnName.isBlank()) continue;

                Map<String, String> column = new LinkedHashMap<>();
                column.put("컬럼명", columnName);
                column.put("데이터타입", dataType);
                column.put("설명", description);

                tableMap.computeIfAbsent(currentTableName, k -> new ArrayList<>()).add(column);
            }

        } catch (Exception e) {
            throw new RuntimeException("엑셀 로딩 실패: " + e.getMessage(), e);
        }

        return tableMap;
    }

    private String getCellValue(Cell cell) {
        return (cell == null) ? "" : cell.toString().trim();
    }
}

