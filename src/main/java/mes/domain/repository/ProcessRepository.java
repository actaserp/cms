package mes.domain.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import mes.domain.entity.Process;

@Repository
public interface ProcessRepository extends JpaRepository<Process, Integer> {
	
	Optional<Process> findByName(String name);
	Optional<Process> findByCode (String code);
	
	Process getProcessById(Integer id);


	@Query("SELECT DISTINCT t.processType FROM Process t WHERE LOWER(t.processType) LIKE LOWER(CONCAT('%', :query, '%'))")
	List<String> findProcessTypeByQuery(@Param("query") String query);

}
