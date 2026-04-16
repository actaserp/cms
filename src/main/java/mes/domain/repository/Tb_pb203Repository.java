package mes.domain.repository;

import mes.domain.entity.Tb_pb203;
import mes.domain.entity.Tb_pb203Id;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface Tb_pb203Repository extends JpaRepository<Tb_pb203, Tb_pb203Id> {


    Optional<Tb_pb203> findByIdSpjangcdAndIdWorkymAndIdPersonid(
            String spjangcd, String workym,Integer personid);

    // 월정산 존재 여부 체크용
    @Query("SELECT COUNT(t) > 0 FROM Tb_pb203 t WHERE t.id.spjangcd = :spjangcd AND t.id.workym = :workym AND t.id.personid = :personid")
    boolean existsByKey(@Param("spjangcd") String spjangcd, @Param("workym") String workym, @Param("personid") Integer personid);

}
