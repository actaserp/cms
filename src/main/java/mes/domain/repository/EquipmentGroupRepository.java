package mes.domain.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.EquipmentGroup;

public interface EquipmentGroupRepository extends JpaRepository<EquipmentGroup, Integer> {
	Optional<EquipmentGroup> findByCode (String code);
	Optional<EquipmentGroup> findByName (String name);
	EquipmentGroup getEquipGroupById(Integer id);

}
