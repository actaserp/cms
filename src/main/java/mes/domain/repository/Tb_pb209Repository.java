package mes.domain.repository;

import mes.domain.entity.Tb_pb209;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface  Tb_pb209Repository extends JpaRepository<Tb_pb209, Integer> {

    Optional<Tb_pb209> findByPersonidAndReqdate(Integer personid, String reqdate);

}
