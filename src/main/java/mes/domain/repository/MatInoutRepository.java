package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MaterialInout;

@Repository
public interface MatInoutRepository extends JpaRepository<MaterialInout, Integer> {
	
	MaterialInout getMatInoutById(Integer id);

	List<MaterialInout> findBySourceTableNameAndSourceDataPkAndInOutAndInputType(String string, int id, String string2,
			String string3);

	List<MaterialInout> findBySourceTableNameAndSourceDataPkAndInOutAndOutputType(String string, int id, String string2,
			String string3);

	MaterialInout findBySourceTableNameAndSourceDataPkAndInOutAndInputTypeAndMaterialId(String string, int id,
			String string2, String string3, Integer materialId);

	MaterialInout findBySourceTableNameAndSourceDataPkAndInOutAndOutputTypeAndMaterialId(String string, int id,
			String string2, String string3, Integer consumeMatPk);

	void deleteBySourceTableNameAndSourceDataPkAndInOutAndInputType(String string, int id, String string2,
			String string3);

	void deleteBySourceTableNameAndSourceDataPkAndInOutAndOutputType(String string, int id, String string2,
			String string3);
}
