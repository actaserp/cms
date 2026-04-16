package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.EquipmentHistory;

public interface EquipmentHistoryRepository extends JpaRepository<EquipmentHistory, Integer> {
	
	EquipmentHistory getEquipmentHistoryById(Integer id);
	
	void deleteByApprDataPk(Integer pk);
	
	List<EquipmentHistory> findBySourceDataPkAndSourceTableName(Integer sourceDataPk, String sourceTableName);

	List<EquipmentHistory> findByEquipmentId(Integer equipmentId);
}
