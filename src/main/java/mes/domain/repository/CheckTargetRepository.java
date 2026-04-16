package mes.domain.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.CheckTarget;

@Repository
public interface CheckTargetRepository extends JpaRepository<CheckTarget, Integer> {

	CheckTarget getCheckTargetById(Integer Id);

	Optional<CheckTarget> findByTargetName(String target_name);
	
	List<CheckTarget> findByCheckMasterId(Integer id);
}
