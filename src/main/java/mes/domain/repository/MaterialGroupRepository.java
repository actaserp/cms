package mes.domain.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MaterialGroup;

@Repository
public interface MaterialGroupRepository extends JpaRepository<MaterialGroup, Integer> {

	//List<MaterialGroup> findByName(String name);
	Optional<MaterialGroup> findByName(String name);
	Optional<MaterialGroup> findByCode(String code);
	MaterialGroup getMatGrpById(Integer id);
	
}
