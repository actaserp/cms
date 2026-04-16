package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.Depart;

import java.util.List;

@Repository
public interface DepartRepository extends JpaRepository<Depart, Integer>{

	Depart getDepartById(Integer id);
	List<Depart> findBySpjangcd(String spjangcd);

}
