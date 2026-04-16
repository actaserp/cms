package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MaterialConsume;

@Repository
public interface MatConsuRepository extends JpaRepository<MaterialConsume, Integer>{

	int countByJobResponseId(Integer jrPk);

	List<MaterialConsume> findByJobResponseId(Integer jrPk);

	List<MaterialConsume> findByJobResponseIdAndMaterialId(Integer jrPk, Integer matPk);

	List<MaterialConsume> findByJobResponseIdAndProcessOrderAndLotIndex(int id, Integer processOrder, Integer lotIndex);

	MaterialConsume getByJobResponseIdAndProcessOrderAndLotIndexAndMaterialId(int id, Integer processOrder, Integer lotIndex,
			int consumeMatPk);

}
