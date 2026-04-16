package mes.app.files;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.shipment.service.TradeStmtService;
import mes.config.Settings;

@RestController
@RequestMapping("/api/files/trade_stmt_excel")
public class TradeStmtExcelController {

	@Autowired
	TradeStmtService tradeStmtService;

	@Autowired
	Settings settings;
	
	@GetMapping("/read")
	public ResponseEntity<?> printTradingStatement(
			@RequestParam(value="head_id") Integer head_id,
			@RequestParam(value="filename") String filename,
			HttpServletRequest request, 
			HttpServletResponse response) throws UnsupportedEncodingException {

		// 템플릿 파일 위치 셋팅 
		String path = settings.getProperty("mes_form_path");
		String filePath = "거래명세서샘플1.xlsx";
		
		// 저장할 데이터 셋팅
		Map<String, Object> header = this.tradeStmtService.getTradeStmtHeaderInfo(head_id);
		List<Map<String, Object>> items = this.tradeStmtService.getTradeStmtItemList(head_id, 0);
		
		// 거래명세서 샘플 파일 설정
		if (items.size() > 12) {
			filePath = "거래명세서샘플2.xlsx";
		}

		// 파일 다운로드 위치 설정
		filePath = path + filePath;
		byte[] excelBuffer = this.tradeStmtService.excel_export(filePath, header, items);
		

		// 파일명을 URLEncoder하여 attachment, Content-disposition Header로 설정
		String encodedFilename = "attachment;filename*=UTF-8" + "''" + URLEncoder.encode(filename, "UTF-8");
		HttpHeaders headers = new HttpHeaders();
		headers.set("Content-type", "application/excel;charset=utf-8");
		headers.set("Content-Disposition", encodedFilename);	
		return ResponseEntity.status(HttpStatus.OK).headers(headers).body(excelBuffer);

	}
}
