package mes.domain.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.DocForm;

@Repository
public interface DocFormRepository extends JpaRepository<DocForm, Integer> {

	Optional<DocForm> findByFormName(String formName);
	
	DocForm getDocFormById(Integer id);

	List<DocForm> findByFormNameAndFormType(String formName, String string);
}
