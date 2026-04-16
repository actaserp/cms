package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.StopCause;

@Repository 
public interface StopCauseRepository extends JpaRepository<StopCause, Integer>{

	StopCause getStopCauseById(Integer id);
	
	List<StopCause> findByStopCauseName(String stopCauseName);

}
