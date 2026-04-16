package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.RoutingProc;

public interface RoutingProcRepository extends JpaRepository<RoutingProc, Integer>{

	Integer countByRoutingId(Integer routingPk);

}
