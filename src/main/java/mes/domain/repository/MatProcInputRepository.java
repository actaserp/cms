package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MatProcInput;

@Repository
public interface MatProcInputRepository extends JpaRepository<MatProcInput, Integer>{

	List<MatProcInput> findByMaterialProcessInputRequestIdAndMaterialId(Integer materialProcessInputRequestId,
			Integer consumeMatPk);

	List<MatProcInput> findByMaterialLotId(int id);

	List<MatProcInput> findByMaterialProcessInputRequestIdAndMaterialLotId(Integer materialProcessInputRequestId,
			int id);

}
