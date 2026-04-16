package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.ShipmentHead;

import java.util.List;

@Repository
public interface ShipmentHeadRepository extends JpaRepository<ShipmentHead, Integer>{

	ShipmentHead getShipmentHeadById(Integer id);

	List<ShipmentHead> findByMisnumIn(List<Integer> misnums);

}
