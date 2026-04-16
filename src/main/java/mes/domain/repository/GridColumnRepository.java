package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.GridColumn;

public interface GridColumnRepository extends JpaRepository<GridColumn, Integer> {

	GridColumn getGridColumnById(Integer id);

	List<GridColumn> findByModuleNameAndTemplateKeyAndGridNameAndKey(String moduleName, String templateName, String gridName, String key);
	
	List<GridColumn> findByModuleNameAndTemplateKeyAndGridName(String moduleName, String templateName, String gridName);
}
