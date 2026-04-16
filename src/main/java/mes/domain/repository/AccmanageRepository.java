package mes.domain.repository;

import mes.domain.entity.Accmanage;
import mes.domain.entity.AccmanageId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;


@Repository
public interface AccmanageRepository extends JpaRepository<Accmanage, AccmanageId> {

    List<Accmanage> findById_Acccd(String acccd); // acccd 기준으로 조회

    void deleteById_Acccd(String acccd);

}
