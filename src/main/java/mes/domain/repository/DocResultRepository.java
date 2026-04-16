package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.DocResult;

@Repository
public interface DocResultRepository extends JpaRepository<DocResult, Integer> {
	
	DocResult getDocResultById(Integer id);

	List<DocResult> findByNumber1AndText1(float bhId, String string);
}
