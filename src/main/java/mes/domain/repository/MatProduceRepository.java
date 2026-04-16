package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MaterialProduce;

@Repository
public interface MatProduceRepository extends JpaRepository<MaterialProduce, Integer> {

	MaterialProduce getMatProduceById(Integer id);

	List<MaterialProduce> findByJobResponseIdAndMaterialId(Integer jrPk, Integer materialId);

	List<MaterialProduce> findByJobResponseId(int id);

	List<MaterialProduce> findByJobResponseIdOrderByLotIndexDesc(Integer jrPk);
}
