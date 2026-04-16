package mes.domain.repository;

import mes.domain.entity.MenuFrontFolder;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface MenuFrontFolderRepository extends JpaRepository<MenuFrontFolder, Integer> {

    @Query("SELECT f FROM MenuFrontFolder f ORDER BY f._order ASC")
    List<MenuFrontFolder> findAllByOrderByOrderAsc();
}
