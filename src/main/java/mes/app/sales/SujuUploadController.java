package mes.app.sales;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import mes.domain.entity.Company;
import mes.domain.entity.Material;
import mes.domain.repository.CompanyRepository;
import mes.domain.repository.MaterialRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import mes.app.sales.service.SujuUploadService;
import mes.config.Settings;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.SujuRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/sales/suju_upload")
public class SujuUploadController {

	@Autowired
	SujuRepository SujuRepository;
	
	@Autowired
	private SujuUploadService sujuUploadService;

	@Autowired
	MaterialRepository materialRepository;

	@Autowired
	CompanyRepository companyRepository;

	@Autowired
	Settings settings;
	
	@Autowired
	SqlRunner sqlRunner;
	
	// 수주 searchDataBind
	@GetMapping("/read")
	public AjaxResult getSujuUploadList(
			@RequestParam(value="date_kind", required=false, defaultValue="sales") String date_kind,
			@RequestParam(value="start", required=false) String start_date,
			@RequestParam(value="end", required=false) String end_date,
			@RequestParam(value="spjangcd", required=false) String spjangcd,
			HttpServletRequest request) {
		
//		start_date = start_date + " 00:00:00";
//		end_date = end_date + " 23:59:59";
//		Timestamp start = Timestamp.valueOf(start_date);
//		Timestamp end = Timestamp.valueOf(end_date);
		
		List<Map<String, Object>> items = this.sujuUploadService.getSujuUploadList(date_kind, start_date, end_date, spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	/**
	 * 엑셀 컬럼 순서
       사업부 - 프로젝트 - 발주번호 - 자재번호 - 품명 - 규격 - 수량 - 단가 - 금액 - 단위 - 발주일 - 요청일
	  	#jumun_number_col = 0   # 주문번호
	    #prod_code_col = 1      # 제품코드
	    #prod_name_col = 2      # 제품명
	    #qty_col = 3            # 수량 
	    #jumnun_date_col = 4    # 주문일
	    #company_code_col = 5   # 업체코드
	    #company_name_col = 6   # 업체명
	    #due_date_col = 7       # 납기일
	
	    #jumun_number_col = 0   # 주문번호
	    #jumnun_date_col = 1    # 주문일
	    #company_code_col = 2   # 업체코드
	    #company_name_col = 3   # 업체명
	    #prod_code_col = 4      # 제품코드
	    #prod_name_col = 5      # 제품명
	    #due_date_col = 7       # 납기일

	 **/
	// 수주 엑셀 업로드
	@PostMapping("/upload_save")
	public AjaxResult saveSujuBulkData(
			@RequestParam(value="data_date") String data_date,
			@RequestParam(value="spjangcd") String spjangcd,
			@RequestParam(value="upload_file") MultipartFile upload_file,
			MultipartHttpServletRequest multipartRequest,
			Authentication auth) throws FileNotFoundException, IOException  {
		
			User user = (User)auth.getPrincipal();
			
			int jumun_number_col = 0;
			int jumnun_date_col = 1;
			int company_code_col = 2;
			int company_name_col = 3;
			int prod_code_col = 4;
			int prod_name_col = 5;
			int qty_col = 6;
			int due_date_col = 7;
			
			DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
			LocalDateTime now = LocalDateTime.now();
			String formattedDate = dtf.format(now);
			String upload_filename = settings.getProperty("file_temp_upload_path") + formattedDate + "_" + upload_file.getOriginalFilename();



			if (new File(upload_filename).exists()) {
			    new File(upload_filename).delete();
			}
			
			try (FileOutputStream destination = new FileOutputStream(upload_filename)) {
			    destination.write(upload_file.getBytes());
			}

			List<List<String>> suju_file = this.sujuUploadService.excel_read(upload_filename);
			//List<String> error_items = new ArrayList<>();
			Map<String, Object> error_items = new HashMap<String, Object>();

		List<Material> materialList = materialRepository.findAll();
		List<Company> CompanyList = companyRepository.findAll();
		AjaxResult result = new AjaxResult();

		for (int i=0; i < suju_file.size(); i++) {

			    List<String> row = suju_file.get(i);
			    
			    String jumun_number = row.get(jumun_number_col);
			    String prod_code = row.get(prod_code_col);
			    String prod_name = row.get(prod_name_col);
			    Float quantity = CommonUtil.tryFloat(row.get(qty_col));
			    String company_code = row.get(company_code_col);
			    String company_name = row.get(company_name_col);
			    String jumun_date_str = row.get(jumnun_date_col);
			    String due_date_str = row.get(due_date_col);

			    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
			    LocalDate jumun_date = LocalDate.parse(jumun_date_str, formatter);
			    LocalDate due_date = LocalDate.parse(due_date_str, formatter);


			boolean exits_material = materialList.stream()
					.anyMatch(material -> prod_code.equals(material.getCode()));
			boolean exits_company = CompanyList.stream()
					.anyMatch(company -> company_code.equals(company.getCode()));

			if(!exits_material){
				result.message = "엑셀에 기입된 제품코드가 없습니다. 제품코드를 등록하고 이용해주세요.";
				result.success = false;
				return result;
			}else if(!exits_company){
				result.message = "엑셀에 기입된 업체코드가 없습니다. 업체코드를 등록하고 이용해주세요.";
				result.success = false;
				return result;
			}

			MapSqlParameterSource paramMap = new MapSqlParameterSource();
				paramMap.addValue("jumun_number", jumun_number);
				paramMap.addValue("jumun_date", jumun_date);
				paramMap.addValue("company_code", company_code);
				paramMap.addValue("company_name", company_name);
				paramMap.addValue("prod_code", prod_code);
				paramMap.addValue("prod_name", prod_name);
				paramMap.addValue("quantity", quantity);
				paramMap.addValue("due_date", due_date);
				paramMap.addValue("creator_id", user.getId());
				paramMap.addValue("spjangcd", spjangcd);

			    String sql =
			        "INSERT INTO public.suju_bulk(" +
			            "\"JumunNumber\", \"JumunDate\", \"CompCode\", \"CompanyName\", \"ProductCode\", \"ProductName\", \"Quantity\", \"DueDate\", _created, _status, _creater_id, spjangcd" +
			        ") VALUES (" +
			            ":jumun_number, :jumun_date, :company_code, :company_name, :prod_code, :prod_name, :quantity, :due_date, now(), 'Excel', :creator_id, :spjangcd "+
			        ")";
			    
			    String log_data =
			            "index:" + i +
			            ", jumun_number:" + jumun_number +
			            ", company_code : " + company_code +
			            ", company_name : " + company_name +
			            ", prod_code : " + prod_code +
			            ", prod_name : " + prod_name +
			            ", quantity: " + quantity.toString() +
			            ", jumun_date: " + jumun_date_str +
			            ", due_date : " + due_date_str;
			    
			    try {
			        if (prod_code != null && !prod_code.isEmpty() && prod_name != null && !prod_name.isEmpty() && quantity > 0) {
			        	this.sqlRunner.execute(sql, paramMap);
			        }
			    } catch (Exception e) {
			        error_items.put("log",log_data);
			        error_items.put("ex",e.getMessage());
			        continue;
			    }
			    
		}
			

		result.success=true;
		
		if( error_items.size() > 0 )
			result.success=false;
		
		Map<String, Object> item = new HashMap<String, Object>();
		item.put("error_items", error_items);
		
		result.data=item;
		return result;
	}
	
	// 수주 변환 changeSujuBulkData
	@PostMapping("/change")
	public AjaxResult changeSujuBulkData(
			@RequestParam MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		User user = (User)auth.getPrincipal();
		
		List<Map<String, Object>> error_items = new ArrayList<>();
	    String sql = "";
	    
		List<Map<String, Object>> qItems = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		if (qItems.size() == 0) {
			result.success = false;
			return result;
		}
		
		for(int i = 0; i < qItems.size(); i++) {
			Integer id = Integer.parseInt(qItems.get(i).get("id").toString());
	    	String state = CommonUtil.tryString(qItems.get(i).get("state"));
	    	
	    	MapSqlParameterSource paramMap = new MapSqlParameterSource();
			paramMap.addValue("id", id);
			paramMap.addValue("user_pk", user.getId());

			//sujuUploadService.BeforeCheck();

			if (state.equals("엑셀")) {
				sql = """
					with A as (
                    select "JumunNumber", m.id as mat_pk, b."Quantity", b."JumunDate"::date, b."DueDate"::date, c.id as comp_pk, b."CompanyName"
                    , m."UnitPrice", case when m."VatExemptionYN" = 'Y' then 0 else 0.1 end as vat_pro
                    from suju_bulk b 
                    inner join material m on m."Code" = b."ProductCode"
                    --left join company c on c."Name" = b."CompanyName"
                    left join company c on c."Code"  = b."CompCode"
                    where b.id = :id
                ), B as (
                    select A.mat_pk, A.comp_pk, mcu."UnitPrice"
                    , row_number() over (partition by A.mat_pk, A.comp_pk order by mcu."ApplyStartDate" desc) as g_idx
                    from mat_comp_uprice mcu
                    inner join A on A.mat_pk = mcu."Material_id"
                    and A.comp_pk = mcu."Company_id"
                    and A."JumunDate" between mcu."ApplyStartDate" and mcu."ApplyEndDate"
                )
                insert into suju("JumunNumber", "Material_id", "SujuQty", "SujuQty2", "JumunDate", "DueDate", "Company_id", "CompanyName"
                , "UnitPrice", "Price", "Vat", "State", _status, _created, _creater_id )
                select A."JumunNumber", A.mat_pk, A."Quantity", A."Quantity", A."JumunDate", A."DueDate", A.comp_pk, A."CompanyName"
                , coalesce(B."UnitPrice", A."UnitPrice") as unit_price
                , coalesce(B."UnitPrice", A."UnitPrice") * a."Quantity" as price
                , A.vat_pro * coalesce(B."UnitPrice", A."UnitPrice") * a."Quantity" as vat
                , 'received', 'excel', now(), :user_pk
                from A 
                left join B on B.mat_pk = a.mat_pk
                and B.comp_pk = A.comp_pk 
                and B.g_idx = 1
				  """;
				this.sqlRunner.execute(sql, paramMap);
				
				sql = """
					update suju_bulk set _status = 'Suju' where id = :id
					  """;
				
				this.sqlRunner.execute(sql, paramMap);
				
	        } else {
	            Map<String, Object> err_item = new HashMap<>();
	            err_item.put("success", false);
	            //err_item.put("message", "Excel상태만 전환할 수 있습니다.");
	            err_item.put("id", id);
	            error_items.add(err_item);
	        }
			
	    }
	
		result.success=true;
		
		if( error_items.size() > 0 ) {
			result.success=false;
			result.message="엑셀 상태만 전환할 수 있습니다.";
		}
		
//		Map<String, Object> item = new HashMap<String, Object>();
//		item.put("error_items", error_items);
//		
//		result.data=item;
		return result;
	}
	
	// 수주 삭제
	@PostMapping("/delete")
	public AjaxResult deleteSujuBulkData(
			@RequestParam MultiValueMap<String,Object> Q,
			HttpServletRequest request) {
		
		AjaxResult result = new AjaxResult();
		
	    String sql = "";
		List<Map<String, Object>> qItems = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		if (qItems.size() == 0) {
			result.success = false;
			return result;
		}
		
		for(int i = 0; i < qItems.size(); i++) {
			Integer id = Integer.parseInt(qItems.get(i).get("id").toString());
	    	String state = CommonUtil.tryString(qItems.get(i).get("state"));
	    	
	    	MapSqlParameterSource paramMap = new MapSqlParameterSource();
			paramMap.addValue("id", id);
			
			if (state.equals("엑셀")) {
				sql = """
					delete from suju_bulk where id = :id
				  """;
				this.sqlRunner.execute(sql, paramMap);
				result.success=true;
				
	        } else {
	        	result.success = false;
	        	result.message="엑셀 상태만 삭제할 수 있습니다.";
	        }
			
	    }
		
		return result;
	}
	
	
}
