package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.PestControlStandard;

@Repository 
public interface PestControlStandardRepository extends JpaRepository<PestControlStandard, Integer> {

	PestControlStandard getPestControlStandardById(Integer id);

}
