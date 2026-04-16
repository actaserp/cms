package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MenuItem;

@Repository
public interface MenuItemRepository extends JpaRepository<MenuItem, Integer> {

	List<MenuItem> findByMenuFolderId(Integer id);
	
	MenuItem findByMenuCode(String MenuCode);

	MenuItem findByMenuFolderIdAndMenuCode(Integer folder_id, String menuCode);
}
