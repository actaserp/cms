package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.Bom;

import java.sql.Timestamp;
import java.util.List;

public interface BomRepository extends JpaRepository<Bom, Integer>{
	public Bom getBomById(int id);

    List<Bom> findAllByStartDate(Timestamp startDate);

    Bom findByMaterialIdAndBomTypeAndVersion(Integer productId, String manufacturing, String s);
}
