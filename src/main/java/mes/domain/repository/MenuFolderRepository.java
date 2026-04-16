package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MenuFolder;

@Repository
public interface MenuFolderRepository extends JpaRepository<MenuFolder, Integer> {
	
	MenuFolder getMenuFolderById(Integer id);
}
