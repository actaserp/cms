package mes.domain.repository;

import mes.domain.entity.TbAs020;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;


@Repository
public interface WorkReportRepository extends JpaRepository<TbAs020, Integer> {

}
