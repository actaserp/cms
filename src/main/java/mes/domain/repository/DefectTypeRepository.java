package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.DefectType;

@Repository 
public interface DefectTypeRepository extends JpaRepository<DefectType, Integer> {

	List<DefectType> findByName (String name);
	
	List<DefectType> findByCode (String code);
	
	DefectType getDefectTypeById(Integer id);
}
