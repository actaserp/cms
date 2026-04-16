package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MatRequ;

@Repository
public interface MatReqRepository extends JpaRepository<MatRequ, Integer>{

	List<MatRequ> findBySourceTableNameAndSourceDataPkAndMaterialType(String SourceTableName, Integer SourceDataPk, String MaterialType);

	MatRequ getMatReqById(Integer id);

}
