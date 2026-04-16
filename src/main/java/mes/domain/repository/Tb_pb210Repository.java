package mes.domain.repository;

import mes.domain.entity.Tb_pb210;
import mes.domain.entity.Tb_pb210Id;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface Tb_pb210Repository extends JpaRepository<Tb_pb210, Tb_pb210Id> {

}
