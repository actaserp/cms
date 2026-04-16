package mes.domain.repository;

import mes.domain.entity.TB_Invoicement;
import mes.domain.entity.TB_Salesment;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface TB_InvoicementRepository extends JpaRepository<TB_Invoicement, Integer>  {


}