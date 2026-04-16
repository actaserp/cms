package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.EquipmentMaint;

public interface EquipmentMaintRepository extends JpaRepository<EquipmentMaint, Integer>{

	EquipmentMaint getEquipmentMaintById(Integer id);

}
