package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.Unit;

@Repository
public interface UnitRepository extends JpaRepository<Unit, Integer> {

	List<Unit> findByName(String name);
	
	Unit getUnitById(Integer id);
	
}
