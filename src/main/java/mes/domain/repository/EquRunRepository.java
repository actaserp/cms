package mes.domain.repository;

import org.apache.ibatis.annotations.Param;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import mes.domain.entity.EquRun;

import java.util.Optional;


@Repository
public interface EquRunRepository extends JpaRepository<EquRun, Integer>{

	EquRun getEquRunById(Integer id);

	@Query("SELECT e FROM EquRun e WHERE e.equipmentId = :equipmentId AND e.workOrderNumber = :orderNum AND e.runState = 'run' AND e.endDate IS NULL ORDER BY e.startDate DESC")
	Optional<EquRun> findLatestRunningByEquipmentAndOrder(@Param("equipmentId") Integer equipmentId, @Param("orderNum") String orderNum);

	@Query("SELECT e FROM EquRun e WHERE e.equipmentId = :equipmentId AND e.workOrderNumber = :orderNum AND e.runState = 'complete' ORDER BY e.endDate DESC")
	Optional<EquRun> findLatestCompleteByEquipmentAndOrder(@Param("equipmentId") Integer equipmentId, @Param("orderNum") String orderNum);

	long countByEquipmentIdAndRunState(Integer equipmentId, String runState);

}
