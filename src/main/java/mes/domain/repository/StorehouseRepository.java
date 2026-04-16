package mes.domain.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.StoreHouse;

@Repository
public interface StorehouseRepository extends JpaRepository<StoreHouse, Integer>{

	List<StoreHouse> findByName(String name);
	
	StoreHouse getStoreHouseById(Integer id);

	Integer countByHouseType(String string);

	StoreHouse findByCode(String string);
	
	List<StoreHouse> findByHouseType(String string);

	Optional<StoreHouse> getByCode(String storehouseCode);

	Optional<StoreHouse> getByName(String storehouseName);

}
