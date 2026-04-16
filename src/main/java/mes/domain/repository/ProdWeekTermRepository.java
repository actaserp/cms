package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.ProdWeekTerm;

@Repository
public interface ProdWeekTermRepository extends JpaRepository<ProdWeekTerm, Integer>{

	ProdWeekTerm getProdWeekTermById(int parseInt);

}
