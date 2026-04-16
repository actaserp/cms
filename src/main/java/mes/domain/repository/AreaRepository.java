package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.Area;

@Repository 
public interface AreaRepository extends JpaRepository<Area, Integer> {
	
	List<Area> findByName (String name);
	
	Area getAreaById(Integer id);
	
}