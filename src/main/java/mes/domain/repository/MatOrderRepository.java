package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MatOrder;

@Repository
public interface MatOrderRepository extends JpaRepository<MatOrder, Integer>{

	MatOrder getMatOrderById(String id);

}
