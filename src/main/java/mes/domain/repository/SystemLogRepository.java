package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.SystemLog;

@Repository
public interface SystemLogRepository extends JpaRepository<SystemLog, Long>{
}
	
