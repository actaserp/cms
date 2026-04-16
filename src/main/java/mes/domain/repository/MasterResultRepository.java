package mes.domain.repository;

import java.sql.Date;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MasterResult;

@Repository
public interface MasterResultRepository extends JpaRepository<MasterResult, Integer>  {
	
	MasterResult getMasterResultById(Integer id);
	
	List<MasterResult> findByIdAndDataDateBetween(Integer id, Date date_from, Date date_to);

	void deleteByMasterClassAndNumber2(String masterClass, Integer number2);

	List<MasterResult> findBySourceDataPkAndSourceTableName(Integer bhId, String string);

}
