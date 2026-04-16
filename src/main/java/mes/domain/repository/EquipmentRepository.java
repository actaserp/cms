package mes.domain.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.Equipment;

@Repository
public interface EquipmentRepository extends JpaRepository<Equipment, Integer>{
	
		Optional<Equipment> findByName(String name);
		Optional<Equipment> findByCode(String code);
		Equipment getEquipmentById(Integer id);
		
		Optional<Equipment> findByCodeAndIdNot(String code, Integer id);
		Optional<Equipment> findByNameAndIdNot(String name, Integer id);
}
