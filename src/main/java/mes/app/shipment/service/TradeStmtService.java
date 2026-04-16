package mes.app.shipment.service;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class TradeStmtService {

	@Autowired
	SqlRunner sqlRunner;

	
	
	public Map<String, Object> getTradeStmtHeaderInfo(int head_id){
		
		
		// 1. header 조회
		// 1) 자기회사 정보
        String sql = """
        select c."Name" as supplier_name 
		, c."TelNumber" as supplier_tel 
		, c."FaxNumber" as supplier_fax
		, c."ZipCode" as supplier_zip
		, c."Address" as supplier_address 
		, c."BusinessNumber" as supplier_busi_number
		, c."CEOName" as supplier_ceo_name
		from company c 
		where c."Code" = 'me';
        """;
        Map<String, Object> supplier_info = this.sqlRunner.getRow(sql, null);

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("head_id", head_id);
		// 2) shipment_head 정보
        sql = """
        		select sh.id
			    , sh."Company_id" as customer_id
	            , c."Name" as customer_name
	            , c."TelNumber" as customer_tel
	            , c."FaxNumber" as customer_fax
	            , c."Address" as customer_address
			    , sh."ShipDate" as ship_date
			    , sh."TotalQty" as total_qty
		        , sh."TotalPrice" as total_price
		        , sh."TotalVat" as total_vat
		        , sh."TotalPrice" + coalesce(sh."TotalVat", 0) as total_price2
		        , sh."Description" as description
	            , sh."State" as state
	            , fn_code_name('shipment_state', sh."State") as state_name
	            , sh."StatementIssuedYN" as issue_yn
	            , sh."StatementNumber" as stmt_number
	            , sh."IssueDate" as issue_date
	            from shipment_head sh 
	            left join company c on c.id = sh."Company_id"   
	            where sh.id =  :head_id
        		""";

        Map<String, Object> header = this.sqlRunner.getRow(sql, paramMap);

        // header 정보 합치기
        if (header != null && supplier_info != null) {
        	header.putAll(supplier_info);
        }
        return header;
	}
	
	public List<Map<String, Object>> getTradeStmtItemList(int head_id, int company_id){
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
        // 2. detail 데이터 조회
        paramMap = new MapSqlParameterSource();
		paramMap.addValue("head_id", head_id);
		paramMap.addValue("companyId", company_id);

        String sql = """
        		select s.id as ship_pk
	            , to_char(sh."ShipDate", 'mm-dd') as data_date
				, s."Material_id" as mat_pk
				, mg."Name" as mat_grp_name
				, m."Code" as mat_code
				, m."Name" as mat_name
	            , '' as mat_spec
				, u."Name" as unit_name
				, s."OrderQty" as order_qty 
				, s."Qty" as ship_qty
				, s."Description" as description 
				,case
				when s."SourceTableName" = 'product' then mcu."UnitPrice"
				else su."UnitPrice"
				end as unit_price
				,TRUNC((
				                       CASE
				                         WHEN s."SourceTableName" = 'product' THEN mcu."UnitPrice" * s."Qty"
				                         WHEN su."InVatYN" = 'Y' THEN (su."UnitPrice" * (10.0 / 11)) * s."Qty"
				                         ELSE su."UnitPrice" * s."Qty"
				                       END
				                     )::numeric, 2) AS price	
				,TRUNC((
				                                 CASE
				                                   WHEN s."SourceTableName" = 'product' THEN (mcu."UnitPrice" * s."Qty") * 0.1
				                                   WHEN su."InVatYN" = 'Y' THEN (su."UnitPrice" - (su."UnitPrice" * (10.0 / 11))) * s."Qty"
				                                   ELSE (su."UnitPrice" * s."Qty") * 0.1
				                                 END
				                               )::numeric, 2) AS vat
	            , m."VatExemptionYN" as vat_exempt_yn
				from shipment  s
				inner join material m on m.id = s."Material_id" 
				inner join mat_grp mg on mg.id = m."MaterialGroup_id"
				left join unit u on u.id = m."Unit_id" 
	            inner join shipment_head sh on sh.id = s."ShipmentHead_id"  
	            left join (	
				             			select distinct on ("Material_id") "Material_id", "UnitPrice"
				             			from mat_comp_uprice
				         				WHERE "Type" = '02'
				             				AND "Company_id" = :companyId
				             				AND "ApplyEndDate" > CURRENT_DATE
				             			order by "Material_id", "ApplyStartDate" desc
				         				) mcu on mcu."Material_id" = s."Material_id" and s."SourceTableName" = 'product'
	            left join suju su on su.id = s."SourceDataPk"	
				where s."ShipmentHead_id" = :head_id
	            order by m."Code", m."Name"        		
        		""";
             
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        return items;
	}

	// 엑셀 다운로드	
	public byte[] excel_export(String filePath, Map<String, Object> info, List<Map<String, Object>> items) {
		
		byte[] bytes = new byte[] {};
		FileInputStream inputStream = null;
		XSSFWorkbook workbook = null;
		
		Integer listCnt = 0;
		int rowCnt = 0;
		
		Double total_price2 = null;
		Integer intTotalPrice2 = null;
		String sum_total_price = null;
		String sumTotalPriceText = "";

		List<String> mat_date_list = new ArrayList<>();
        List<String> mat_name_list = new ArrayList<>();
        List<Double> mat_qty_list =  new ArrayList<>();
        List<Double> mat_unit_price_list =  new ArrayList<>();
        List<Double> mat_price_list =  new ArrayList<>();
        List<Double> mat_vat_list = new ArrayList<>();
		
        ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
		try {
			
			// 1. FileInputStream 으로 파일 읽기
			inputStream = new FileInputStream(filePath);	
			
			// 2. XSSFWorkbook 객체 생성하기
		    workbook = new XSSFWorkbook(inputStream);	
		     
			// 3. XSSFSheet 객체 생성 - 첫번째 시트를 가져오기 
			XSSFSheet sheet = workbook.getSheetAt(0);

			// 4. 데이터를 엑셀 파일에 셋팅
			listCnt = items.size();
			
			// 4.1. 기본정보						
			if (info.get("total_price2") != null && info.get("total_price2") != "") {
				// 전체금액
				total_price2 = (Double) info.get("total_price2");
				intTotalPrice2 = total_price2.intValue();
				
				// 전체금액 텍스트
				sum_total_price = String.valueOf(intTotalPrice2);
				sumTotalPriceText = "금  액 :" + makePriceToStr(sum_total_price) + "정";
			} else {
				// 전체금액 텍스트
				sumTotalPriceText = "금  액 :   원 정";
			}

			setCellValue(sheet, 0, 13, info.get("stmt_number"), "string");			// 명세서번호
			setCellValue(sheet, 2, 13, info.get("supplier_name"), "string");		// 공급자회사명
			setCellValue(sheet, 1, 13, info.get("supplier_busi_number"), "string");	// 공급자사업자번호
			setCellValue(sheet, 0, 17, info.get("supplier_tel"), "string");			// 공급자전화번호
			setCellValue(sheet, 1, 17, info.get("supplier_ceo_name"), "string");	// 공급자대표
			setCellValue(sheet, 3, 13, info.get("supplier_address"), "string");		// 공급자주소
			setCellValue(sheet, 2, 0, info.get("customer_name"), "string");			// 수신처
			setCellValue(sheet, 4, 0, info.get("customer_tel"), "string");			// 수신처전화번호
			if (listCnt <= 12) {
				setCellValue(sheet, 22, 1, info.get("total_qty"), "double");		// 전체수량
				setCellValue(sheet, 22, 4, info.get("total_price"), "double");		// 전체공급가액
				setCellValue(sheet, 22, 6, info.get("total_vat"), "double");		// 전체VAT
            } else {
            	setCellValue(sheet, 53, 1, info.get("total_qty"), "double");		// 전체수량
    			setCellValue(sheet, 53, 4, info.get("total_price"), "double");		// 전체공급가액
    			setCellValue(sheet, 53, 6, info.get("total_vat"), "double");		// 전체VAT
            }
			setCellValue(sheet, 6, 16, intTotalPrice2, "integer");		// 전체금액          
			setCellValue(sheet, 6, 1, sumTotalPriceText, "string");		// 전체금액 텍스트
			
			// 4.2. 품목정보	
			for (int i = 0; i < listCnt; i++) {
				 Map<String, Object> item = items.get(i);
				 String data_date = item.get("data_date") != null ? (String) item.get("data_date") : null;
				 String mat_name = item.get("mat_name") != null ? (String) item.get("mat_name") : null;
				 Double order_qty = item.get("order_qty") != null ? (Double) item.get("order_qty") : null;
				 Double unit_price = item.get("unit_price") != null ? (Double) item.get("unit_price") : null;
				 Double price = item.get("price") != null ? (Double) item.get("price") : null;
				 Double vat = item.get("vat") != null ? (Double) item.get("vat") : null;
				 
				 mat_date_list.add(data_date);			// 품목_일자
				 mat_name_list.add(mat_name);           // 품목명_규격
				 mat_qty_list.add(order_qty);			// 품목_수량
				 mat_unit_price_list.add(unit_price);	// 품목_단가
				 mat_price_list.add(price);				// 품목_공급가
				 mat_vat_list.add(vat);					// 품목_부가세
			}
            
			if (listCnt <= 12) {
            	rowCnt = 12;
            } else {
            	rowCnt = 43;
            }
			
			setCellMultiValue(sheet, 0, mat_date_list, "string", rowCnt);			// 품목_일자
			setCellMultiValue(sheet, 2, mat_name_list, "string", rowCnt);			// 품목명_규격
			setCellMultiValue(sheet, 11, mat_qty_list, "double", rowCnt);			// 품목_수량
			setCellMultiValue(sheet, 14, mat_unit_price_list, "double", rowCnt);	// 품목_단가
			setCellMultiValue(sheet, 15, mat_price_list, "double", rowCnt);			// 품목_공급가
			setCellMultiValue(sheet, 18, mat_vat_list, "double", rowCnt);			// 품목_부가세
							
			// 6. 엑셀 파일에 수식이 있는경우 적용되도록   
			FormulaEvaluator evaluator  = workbook.getCreationHelper().createFormulaEvaluator();
			evaluator.evaluateAll();
			
			workbook.write(byteArrayStream);
			bytes = byteArrayStream.toByteArray();
		} catch (Exception e) {
			 System.out.println(e.toString());
		} finally {
			
			if (workbook != null) {
				try {
					workbook.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			try {
				byteArrayStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return bytes;
	}

	// 금액을 한글로 변환
	private String makePriceToStr(String price) {
		
        String[] han1 = { "", "일","이","삼", "사", "오", "육", "칠", "팔", "구" };
        String[] han2 = { "", "십", "백", "천" };
        String[] han3 = { "", "만", "억", "조", "경" };
 
        String result = "";
        
        int length = price.length();
        int initInt = 0;
        
        for (int i = length-1; i >= 0; i--) {
        	
            initInt = Integer.parseInt(String.valueOf(price.charAt(length - i - 1)));
 
            if (initInt > 0) {
                result = result + han1[initInt];
                result = result + han2[i % 4];		// 십,백,천
            }
            
            // 만, 억, 조, 경 단위
            if (i % 4 == 0) {
                result = result + han3[i / 4];		// 천단위
                result = result + " ";
            }            
        }
        
        result = result + "원";
        
        return result;
    }
	
	//엑셀 컬럼에 셋팅
	private void setCellValue(Sheet sheet, Integer rowNum, Integer cellNum, Object cellValue, String cellType) {
		
		Row row = sheet.getRow(rowNum);
		Cell cell = row.getCell(cellNum);
		
		if (cellValue != null && cellValue != "") {
			switch (cellType) {
				case "string":
					cell.setCellValue((String) cellValue);
					break;
				case "double":
					cell.setCellValue((Double) cellValue);	
					break;
				case "integer":
					cell.setCellValue((Integer) cellValue);	
					break;
				default:
					cell.setCellValue("");
					break;
			}
		} else {
			cell.setCellValue("");
		}
	}

	// 엑셀 컬럼에 셋팅
	@SuppressWarnings({ "unchecked" })
	private void setCellMultiValue(Sheet sheet, Integer cellNum, Object cellList, String cellType, int rowCnt) {
		
		for (int rowNum = 0; rowNum <= rowCnt; rowNum++) {
			
			Row row = sheet.getRow(rowNum + 9);
			Cell cell = row.getCell(cellNum);
			
			if (cellList != null) {
				
				switch (cellType) {					
					case "string":
						List<String> stringValue = (List<String>) cellList;
						
						if (rowNum < stringValue.size()) {
							cell.setCellValue(stringValue.get(rowNum));
						} else {
							 cell.setCellValue("");
						}
						break;
					case "double":
						List<Double> doubleValue = (List<Double>) cellList;
						
						if (rowNum < doubleValue.size()) {
							if (doubleValue.get(rowNum) != null) {
								cell.setCellValue(doubleValue.get(rowNum));
							} else {
								cell.setCellValue("");
							}
						} else {
							 cell.setCellValue("");
						}
						break;
					case "integer":
						List<Integer> integerValue = (List<Integer>) cellList;
						
						if (rowNum < integerValue.size()) {
							if (integerValue.get(rowNum) != null) {
								cell.setCellValue(integerValue.get(rowNum));
							} else {
								cell.setCellValue("");
							}
						} else {
							cell.setCellValue("");
						}
						break;
					default:
						cell.setCellValue("");
						break;
				}
			} else {
				 cell.setCellValue("");
			}
		}
	}
	
}
