package mes.domain.repository;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.CheckMaster;

@Repository
public interface CheckMasterRepository extends JpaRepository<CheckMaster, Integer> {
	
	CheckMaster getCheckMasterById(Integer id);
	
	Optional<CheckMaster> findByCode(String code);

	CheckMaster getByCode(String string);

	List<CheckMaster> findCheckMasterById(Integer checkMasterId);

	List<CheckMaster> findByName(String string);
	

}
