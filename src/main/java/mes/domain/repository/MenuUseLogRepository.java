package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MenuUseLog;

@Repository
public interface MenuUseLogRepository extends JpaRepository<MenuUseLog, Integer> {
	
	MenuUseLog getMenuUseLogById(Integer id);
}
