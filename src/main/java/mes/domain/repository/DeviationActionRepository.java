package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.DeviationAction;


@Repository
public interface DeviationActionRepository extends JpaRepository<DeviationAction, Integer> {

	DeviationAction getDeviationActionById(Integer id);
	
	List<DeviationAction> findBySourceTableNameAndSourceDataPk(String sourceTableName, Integer id);

	void deleteBySourceDataPk(Integer sourceDataPk);
}
