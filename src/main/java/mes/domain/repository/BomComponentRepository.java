package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.BomComponent;

import java.util.Optional;

public interface BomComponentRepository extends JpaRepository<BomComponent, Integer> {
	public BomComponent getBomComponentById(int id);

    Optional<BomComponent> findByBomIdAndMaterialId(int bomId, int materialId);

    void deleteByBomId(int bomId);
}
