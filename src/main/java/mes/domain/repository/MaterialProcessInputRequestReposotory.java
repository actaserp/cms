package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MaterialProcessInputRequest;

@Repository
public interface MaterialProcessInputRequestReposotory extends JpaRepository<MaterialProcessInputRequest, Integer> {
	
	MaterialProcessInputRequest getMatProcInputReqById(Integer id);
}
