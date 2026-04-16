package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MatRequ;

@Repository
public interface MatRequRepository extends JpaRepository<MatRequ, Integer> {

	MatRequ getMatRequById(Integer id);
	
	List<MatRequ> findBySourceTableNameAndSourceDataPkAndMaterialType(String SourceTableName, Integer SourceDataPk, String MaterialType);
}
