package mes.domain.repository;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.HaccpTest;
@Repository 
public interface  HaccpTestRepository extends JpaRepository<HaccpTest, Integer>{
	HaccpTest getHaccpTestById(int id);
}
