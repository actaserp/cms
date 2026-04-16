package mes.domain.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.Shift;

@Repository 
public interface ShiftRepository extends JpaRepository<Shift, Integer> {
	
	Optional<Shift> findByCode (String code);
	Shift getShiftById(Integer id);

}
