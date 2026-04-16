package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.TaskApprover;

@Repository 
public interface TaskApproverRepository extends JpaRepository<TaskApprover, Integer>{

	List<TaskApprover> findByTaskMasterIdAndLine(Integer taskmater_id, Integer line);
	
}
