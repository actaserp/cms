package mes.domain.repository;

//import java.util.List;

import mes.domain.entity.Suju;
import mes.domain.entity.SujuHead;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Repository
public interface SujuHeadRepository extends JpaRepository<SujuHead, Integer>{

	Optional<SujuHead> findByJumunNumberAndSpjangcd(String jumunNumber, String spjangcd);

}
