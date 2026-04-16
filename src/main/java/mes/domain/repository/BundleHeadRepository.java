package mes.domain.repository;

import java.sql.Timestamp;
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.BundleHead;

@Repository 
public interface BundleHeadRepository extends JpaRepository<BundleHead, Integer> {
	
	BundleHead getBundleHeadById(Integer id);

	List<BundleHead> findByTableNameAndDate1(String string, Timestamp checkDate);
}
