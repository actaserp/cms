package mes.domain.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.LabelCode;

@Repository
public interface LabelCodeRepository extends JpaRepository<LabelCode, Integer>{

	LabelCode getLabelCodeById(Integer id);

	Optional<LabelCode> findByLabelCode(String labelCode);

}
