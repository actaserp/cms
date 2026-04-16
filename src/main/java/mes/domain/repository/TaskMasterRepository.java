package mes.domain.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.TaskMaster;

@Repository 
public interface TaskMasterRepository extends JpaRepository<TaskMaster, Integer>{

	TaskMaster getTaskMasterById(Integer id);

	Optional<TaskMaster> findByCode(String code);

	TaskMaster findTaskMasterByCode(String taskCode);
	
}
