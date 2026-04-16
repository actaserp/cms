package mes.domain.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.Workcenter;

@Repository
public interface WorkcenterRepository extends JpaRepository<Workcenter, Integer> {
	
	List<Workcenter> findByName(String name);
	
	Workcenter getWorkcenterById(Integer id);

	Optional<Workcenter> findByCode(String code);

}
