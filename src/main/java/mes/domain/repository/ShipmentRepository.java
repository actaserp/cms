package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.Shipment;

@Repository
public interface ShipmentRepository extends JpaRepository<Shipment, Integer>{

	Shipment getShipmentById(Integer id);

	void deleteByShipmentHeadId(Integer head_id);
	
	List<Shipment> findByShipmentHeadId(Integer head_id);

	Shipment getByShipmentHeadId(Integer sh_id);
}
