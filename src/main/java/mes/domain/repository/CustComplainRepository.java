package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.CustComplain;

@Repository
public interface CustComplainRepository extends JpaRepository<CustComplain, Integer>{

	CustComplain getCustComplainById(Integer id);

}
