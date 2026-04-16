package mes.app.system;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.system.service.SystemService;
import mes.domain.entity.GridColLang;
import mes.domain.entity.GridColumn;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.GridColLangRepository;
import mes.domain.repository.GridColumnRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;


@RestController
public class GridController {

	@Autowired
	SqlRunner sqlRunner;

	@Autowired
	SystemService systemService;
	
	@Autowired
	GridColumnRepository gridColumnRepository;
	
	@Autowired
	GridColLangRepository gridColLangRepository;
	
	@GetMapping("/api/system/grid_setting/load_columns")
	public AjaxResult loadGridColumns(
			@RequestParam("module_name") String moduleName, 
			@RequestParam("template_name") String templateName, 
			@RequestParam("grid_name") String gridName,
			@RequestParam("lang_code") String langCode
			) {
		AjaxResult result = new AjaxResult();
		result.data = this.systemService.getGridColumnList(moduleName, templateName, gridName, langCode);
		return result;
	}
	
	@PostMapping("/api/system/grid_setting/save_columns")
	@Transactional
	public AjaxResult saveColums(
			@RequestParam("module_name") String moduleName, 
			@RequestParam(value="template_name", defaultValue="default") String templateName, 
			@RequestParam("grid_name") String gridName,
			@RequestParam("lang_code") String langCode,
			@RequestParam("Q") String strColumns,
			Authentication auth
			) {
		User user = (User)auth.getPrincipal();
		
		List<Map<String, Object>> columns = CommonUtil.loadJsonListMap(strColumns);
		
		AtomicInteger atomicIndex = new AtomicInteger();
		
		
		columns.forEach(map-> {
			atomicIndex.getAndIncrement();
			
			String key = map.get("key").toString();			
			String label = map.get("label").toString();
//			if(StringUtils.hasText(label)) {
//				label = (String)map.get("label");
//			}
			
			int index = atomicIndex.get();
			Integer width = Integer.parseInt(map.get("width").toString());						
			String strHidden = map.get("hidden").toString();
			
			String hidden="";
			if("true".equals(strHidden) | "1".endsWith(strHidden)) {
				hidden = "Y";
			}
			
			
			List<GridColumn> column = this.gridColumnRepository.findByModuleNameAndTemplateKeyAndGridNameAndKey(moduleName, templateName, gridName, key);
			
			GridColumn gridCol = null;
			
			if(column.size() > 0) {
				gridCol = column.get(0);
			} else {
				gridCol =new GridColumn();
			}
			
			gridCol.setModuleName(moduleName);
			gridCol.setTemplateKey(templateName);
			gridCol.setGridName(gridName);
			gridCol.setKey(key);
			gridCol.setHidden(hidden);
			gridCol.setIndex(index);
			gridCol.setWidth(width);
			gridCol.setLabel(label);
			gridCol.set_audit(user);
			
			gridCol = this.gridColumnRepository.save(gridCol);
			
			Integer gridColPk = gridCol.getId();
			
			GridColLang colLang = this.gridColLangRepository.findByGridColumnIdAndLangCode(gridColPk,langCode);
			
			if(colLang == null) {
				colLang = new GridColLang();
				colLang.setGridColumnId(gridColPk);
				colLang.setLangCode(langCode);
			} 
			
			String text = map.containsKey("text") ? map.get("text").toString() : null;
			
			if (text == null) text = label;
			
			colLang.setDispText(text);
			colLang.set_audit(user);
			colLang = this.gridColLangRepository.save(colLang);
			
		});
		
		AjaxResult result = new AjaxResult();
		return result;
	}	
	
	@PostMapping("/api/system/grid_setting/delete")
	@Transactional
	public AjaxResult deleteGridColumns(
			@RequestParam(value = "module_name", required = false) String module_name,
			@RequestParam(value = "template_name", required = false) String template_name,
			@RequestParam(value = "grid_name", required = false) String grid_name) {
		
		AjaxResult result = new AjaxResult();
		
		if (template_name == null || template_name == "") {
			template_name = "default";
		}
		
		List<GridColumn> data = this.gridColumnRepository.findByModuleNameAndTemplateKeyAndGridName(module_name, template_name, grid_name);
		
		if (data.size() > 0) {
			
			for (int i = 0; i < data.size(); i++) {

				Integer id = data.get(i).getId();
				
				if (id != null && id > 0) {
					
					// grid_col_lang
					this.gridColLangRepository.deleteById(id);
					
					// grid_col
					this.gridColumnRepository.deleteById(id);
				}
			}
		}
		
		return result;
	}
	
}
