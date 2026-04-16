package mes.app.support.service;

import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class MaterialOptimalStockStatusServicr {
  @Autowired
  SqlRunner sqlRunner;

//  public List<Map<String, Object>> getList(String matName, String status,Integer store_id, Timestamp start, Timestamp end, String spjangcd) {
//
//    MapSqlParameterSource paramMap = new MapSqlParameterSource();
//    paramMap.addValue("matName", matName);
//    paramMap.addValue("status", status);
//    paramMap.addValue("store_id", store_id);
//    paramMap.addValue("start", start);
//    paramMap.addValue("end", end);
//    paramMap.addValue("spjangcd", spjangcd);
//    //-- :horizon_days := 14, :usage_days := 90
//
//    // 기본값을 SQL COALESCE로 처리할 때는 null로라도 바인딩 필요
//    paramMap.addValue("usage_days", null);        // 없으면 90으로 동작
//    paramMap.addValue("horizon_days", null);      // 없으면 14로 동작
//
//    String sql = """
//        WITH
//        -- 0) 자재 마스터
//        mat AS (
//          SELECT
//            m.id,
//            m."Code"  AS code,
//            m."Name"  AS name,
//            m."Unit_id",
//             m.spjangcd,
//            COALESCE(m."PackingUnitName",'') AS packing_unit_name,
//            COALESCE(m."SafetyStock", 0)::numeric AS safety_stock,          -- 안전재고
//            GREATEST(COALESCE(m."LeadTime", 0), 0) AS leadtime_days,        -- 일 단위
//            LOWER(TRIM(COALESCE(m."PurchaseOrderStandard", ''))) AS order_type  -- 'mrp'/'rop' 또는 ''/null (소문자 정규화)
//          FROM material m
//        ),
//        -- 1) 현재고(창고 합계)
//        stock AS (
//          SELECT
//            h."Material_id" AS mat_id,
//            COALESCE(SUM(h."CurrentStock"), 0)::numeric AS current_stock
//          FROM mat_in_house h
//          GROUP BY h."Material_id"
//        ),
//        -- 2) 최근 입고일
//        last_receipt_ranked AS (
//            SELECT
//              io."Material_id"   AS mat_id,
//              io."StoreHouse_id" AS store_house_id,
//              s."Name"           AS house_name,
//              io."InoutDate"     AS last_in_date,
//              ROW_NUMBER() OVER (PARTITION BY io."Material_id" ORDER BY io."InoutDate" DESC) AS rn
//            FROM mat_inout io
//            LEFT JOIN store_house s ON s.id = io."StoreHouse_id"
//            WHERE lower(io."InOut") = 'in'
//              AND io."InoutDate" >= COALESCE(CAST(:start AS date), io."InoutDate")
//              AND io."InoutDate" <= COALESCE(CAST(:end   AS date), io."InoutDate")
//          ),
//          last_receipt AS (
//            SELECT mat_id, store_house_id, house_name, last_in_date
//            FROM last_receipt_ranked
//            WHERE rn = 1
//          ),
//        -- 3) BOM 자식 여부 (자재가 BOM 구성품이면 MRP 기본)
//        bom_child AS (
//          SELECT DISTINCT bc."Material_id" AS mat_id
//          FROM bom_comp bc
//        ),
//        -- 4) ROP/보정용 '실제 사용' 기반 평균 일일소요
//        -- 최근 :usage_days 동안의 생산 실적(GoodQty) × BOM 소요(Amount) = 실제 소모 원자재 수량
//        actual_usage_jobres AS (
//          SELECT
//            bc."Material_id" AS mat_id,                                      -- 자식(원자재)
//            COALESCE(SUM(COALESCE(jr."GoodQty",0) * bc."Amount"),0)::numeric AS used_qty
//          FROM job_res jr
//          JOIN bom b
//            ON b."Material_id" = jr."Material_id"                            -- jr: 상위(제품/반제품)
//          JOIN bom_comp bc
//            ON bc."BOM_id" = b.id                                            -- bc: 자식(원자재)
//          WHERE jr."ProductionDate" >= (CURRENT_DATE - (:usage_days || ' days')::interval)
//            AND jr."ProductionDate" <  (CURRENT_DATE + INTERVAL '1 day')
//          GROUP BY bc."Material_id"
//        ),
//        daily_usage AS (
//          SELECT
//            u.mat_id,
//            CASE WHEN :usage_days <= 0 THEN 0
//                 ELSE u.used_qty / :usage_days
//            END AS avg_daily_demand
//          FROM actual_usage_jobres u
//        ),
//        -- 5) 생산계획 기간 계산 (job_plan_head의 문자형 날짜를 안전하게 date로 변환)
//        plan_head AS (
//          SELECT
//            h.id,
//            to_date(regexp_replace(h.stdate,'[^0-9]','','g'),'YYYYMMDD') AS stdate,
//            to_date(regexp_replace(h.eddate,'[^0-9]','','g'),'YYYYMMDD') AS eddate
//          FROM job_plan_head h
//        ),
//        -- 6) 커버기간(오늘 ~ 오늘 + :horizon_days)과 겹치는 계획만 추출
//        plan_window AS (
//          SELECT
//            p."material_id"   AS product_id,
//            COALESCE(p.qty,0) AS plan_qty
//          FROM job_plan p
//          JOIN plan_head h ON h.id = p.head_id
//          WHERE h.stdate <= (CURRENT_DATE + (:horizon_days || ' days')::interval)
//            AND h.eddate >= CURRENT_DATE
//        ),
//        /* 7) MRP 총소요량 = Σ(계획수량 × BOM.Amount) */
//        mrp_gross AS (
//          SELECT
//            bc."Material_id" AS mat_id,                                  -- 자식 자재
//            COALESCE(SUM(pw.plan_qty * bc."Amount"),0)::numeric AS gross_req
//          FROM plan_window pw
//          JOIN bom b  ON b."Material_id" = pw.product_id
//          JOIN bom_comp bc ON bc."BOM_id" = b.id
//          GROUP BY bc."Material_id"
//        ),
//        -- 7-1) 계획이 없을 때 실사용 × 커버일수로 보정
//        mrp_gross_eff AS (
//          SELECT
//            m.id AS mat_id,
//            COALESCE(g.gross_req, COALESCE(d.avg_daily_demand,0) * :horizon_days)::numeric AS gross_req_eff
//          FROM mat m
//          LEFT JOIN mrp_gross g ON g.mat_id = m.id
//          LEFT JOIN daily_usage d ON d.mat_id = m.id
//        ),
//        -- 8) 발주구분 자동결정: 지정값 우선, 없으면 BOM 자식이면 mrp, 아니면 rop
//        order_type_resolved AS (
//          SELECT
//            m.id,
//            CASE
//              WHEN m.order_type IN ('mrp','rop') THEN m.order_type
//              WHEN bc.mat_id IS NOT NULL         THEN 'mrp'
//              ELSE 'rop'
//            END AS order_type_resolved
//          FROM mat m
//          LEFT JOIN bom_child bc ON bc.mat_id = m.id
//        )
//        SELECT
//          m.code                        AS "material_code",
//          m.name                        AS "material_name",
//          COALESCE(u."Name", m.packing_unit_name) AS "unit_name",    -- unit 테이블 없으면 packing_unit_name 사용
//          COALESCE(s.current_stock, 0)  AS "current_stock",
//          -- 적정재고 (발주구분 분기)
//          CASE otr.order_type_resolved
//            WHEN 'mrp' THEN COALESCE(ge.gross_req_eff,0) + m.safety_stock
//            ELSE m.safety_stock + COALESCE(d.avg_daily_demand,0) * GREATEST(m.leadtime_days,0)
//          END AS "optimal_stock",
//          -- 차이재고
//          (COALESCE(s.current_stock,0) -
//             CASE otr.order_type_resolved
//               WHEN 'mrp' THEN COALESCE(ge.gross_req_eff,0) + m.safety_stock
//               ELSE m.safety_stock + COALESCE(d.avg_daily_demand,0) * GREATEST(m.leadtime_days,0)
//             END
//          ) AS "diff_stock",
//          -- 상태
//          CASE
//            WHEN COALESCE(s.current_stock,0) >
//                 (CASE otr.order_type_resolved
//                    WHEN 'mrp' THEN COALESCE(ge.gross_req_eff,0) + m.safety_stock
//                    ELSE m.safety_stock + COALESCE(d.avg_daily_demand,0) * GREATEST(m.leadtime_days,0)
//                  END) THEN '과잉'
//            WHEN COALESCE(s.current_stock,0) =
//                 (CASE otr.order_type_resolved
//                    WHEN 'mrp' THEN COALESCE(ge.gross_req_eff,0) + m.safety_stock
//                    ELSE m.safety_stock + COALESCE(d.avg_daily_demand,0) * GREATEST(m.leadtime_days,0)
//                  END) THEN '적정'
//            ELSE '부족'
//          END AS "state",
//          r.last_in_date                AS "last_in_date",
//          r.house_name,
//          otr.order_type_resolved       AS "order_type_used"     -- 실제 적용된 분기 확인용
//        FROM mat m
//        LEFT JOIN stock s                 ON s.mat_id = m.id
//        LEFT JOIN last_receipt r          ON r.mat_id = m.id
//        LEFT JOIN daily_usage d           ON d.mat_id = m.id
//        LEFT JOIN mrp_gross_eff ge        ON ge.mat_id = m.id
//        LEFT JOIN order_type_resolved otr ON otr.id   = m.id
//        LEFT JOIN "unit" u                ON u.id     = m."Unit_id"
//        where m.spjangcd = :spjangcd
//       and r."last_in_date" BETWEEN :start AND :end
//        """;
//
//    if (matName != null && !matName.isEmpty()) {
//      sql += " AND  m.name like :matName ";
//      paramMap.addValue("matName", '%' + matName + '%');
//    }
//    if (store_id != null) {
//      sql += " and r.store_house_id = :store_id ";
//      paramMap.addValue("store_id",  store_id );
//    }
//
//    if (status != null && !status.isBlank() && !"전체".equals(status)) {
//      status = status.trim().toLowerCase();
//
//      // 영문 코드 → 한글 상태 매핑
//      switch (status) {
//        case "excess": status = "과잉"; break;
//        case "proper": status = "적정"; break;
//        case "tribe":  status = "부족"; break;
//        // 혹시나 한글이 들어온 경우도 그냥 그대로 사용
//      }
//
//      sql += """
//    AND (
//      CASE
//        WHEN COALESCE(s.current_stock,0) >
//             (CASE otr.order_type_resolved
//                WHEN 'mrp' THEN COALESCE(ge.gross_req_eff,0) + m.safety_stock
//                ELSE m.safety_stock + COALESCE(d.avg_daily_demand,0) * GREATEST(m.leadtime_days,0)
//             END) THEN '과잉'
//        WHEN COALESCE(s.current_stock,0) =
//             (CASE otr.order_type_resolved
//                WHEN 'mrp' THEN COALESCE(ge.gross_req_eff,0) + m.safety_stock
//                ELSE m.safety_stock + COALESCE(d.avg_daily_demand,0) * GREATEST(m.leadtime_days,0)
//             END) THEN '적정'
//        ELSE '부족'
//      END
//    ) = :status
//  """;
//
//      paramMap.addValue("status", status);  // ✅ 한글 상태가 들어감
//    }
//
//
//    sql += """
//         ORDER BY m.code
//         """;
//
//    log.info("paramMap:{}", paramMap);
//    log.info("자재 적정재고 현황 sql:{}", sql);
//    return sqlRunner.getRows(sql, paramMap);
//  }

  public List<Map<String, Object>> getList(String matName, String status, Integer store_id,
                                           Timestamp start, Timestamp end, String spjangcd) {

    MapSqlParameterSource paramMap = new MapSqlParameterSource();
    paramMap.addValue("matName", matName);
    paramMap.addValue("status", status);
    paramMap.addValue("store_id", store_id);
    paramMap.addValue("start", start);
    paramMap.addValue("end", end);
    paramMap.addValue("spjangcd", spjangcd);

    String sql = """
        WITH RECURSIVE
        -- 0) 자재 마스터 (+ Avrqty 숫자 변환)
        mat AS (
          SELECT
            m.id,
            m."Code"  AS code,
            m."Name"  AS name,
            m."Unit_id",
            m.spjangcd,
            COALESCE(m."PackingUnitName",'') AS packing_unit_name,
            COALESCE(m."SafetyStock", 0)::numeric AS safety_stock,
            GREATEST(COALESCE(m."LeadTime", 0), 0) AS leadtime_days,
            LOWER(TRIM(COALESCE(m."PurchaseOrderStandard", ''))) AS order_type, -- 표시용
            COALESCE(
              NULLIF(regexp_replace(m."Avrqty", '[^0-9.\\-]', '', 'g'), ''),
              '0'
            )::numeric AS avrqty_num
          FROM material m
        ),
        -- 1) 현재고(창고 합계)
        stock AS (
          SELECT
            h."Material_id" AS mat_id,
            COALESCE(SUM(h."CurrentStock"), 0)::numeric AS current_stock
          FROM mat_in_house h
          GROUP BY h."Material_id"
        ),
        -- 2) 최근 입고일(:start ~ :end 사이의 최종 입고)
        last_receipt_ranked AS (
          SELECT
            io."Material_id"   AS mat_id,
            io."StoreHouse_id" AS store_house_id,
            s."Name"           AS house_name,
            io."InoutDate"     AS last_in_date,
            ROW_NUMBER() OVER (PARTITION BY io."Material_id" ORDER BY io."InoutDate" DESC) AS rn
          FROM mat_inout io
          LEFT JOIN store_house s ON s.id = io."StoreHouse_id"
          WHERE lower(io."InOut") = 'in'
            AND io."InoutDate" >= COALESCE(CAST(:start AS date), io."InoutDate")
            AND io."InoutDate" <= COALESCE(CAST(:end   AS date), io."InoutDate")
        ),
        last_receipt AS (
          SELECT mat_id, store_house_id, house_name, last_in_date
          FROM last_receipt_ranked
          WHERE rn = 1
        ),
        -- 3) BOM 자식 여부 (표시용)
         bom_edges AS (
            SELECT
              b."Material_id"  AS parent_id,
              bc."Material_id" AS child_id
            FROM bom b
            JOIN bom_comp bc ON bc."BOM_id" = b.id
          ),
          bom_desc AS (
            -- 1레벨
            SELECT
              e.parent_id,
              e.child_id,
              1 AS lvl,
              ARRAY[e.parent_id, e.child_id] AS path
            FROM bom_edges e
            UNION ALL
            -- 2레벨+
            SELECT
              d.parent_id,
              e.child_id,
              d.lvl + 1 AS lvl,
              d.path || e.child_id
            FROM bom_desc d
            JOIN bom_edges e
              ON e.parent_id = d.child_id
            WHERE NOT e.child_id = ANY(d.path)  -- 사이클 방지
          ),
          bom_child AS (
            -- 어떤 상위품 트리에서든 "자식으로 등장"한 자재 (최소 레벨 포함)
            SELECT
              child_id AS mat_id,
              MIN(lvl) AS min_child_level
            FROM bom_desc
            GROUP BY child_id
          ),
          -- 4) 발주구분 자동결정(표시용)
              order_type_resolved AS (
            SELECT
              m.id,
              CASE
                WHEN m.order_type IN ('mrp','rop') THEN m.order_type
                WHEN bc.mat_id IS NOT NULL         THEN 'mrp'
                ELSE 'rop'
              END AS order_type_resolved,
              bc.min_child_level                   -- (선택) 표시용
            FROM mat m
            LEFT JOIN bom_child bc ON bc.mat_id = m.id
          )
        SELECT
          m.code                        AS "material_code",
          m.name                        AS "material_name",
          COALESCE(u."Name", m.packing_unit_name) AS "unit_name",
          COALESCE(s.current_stock, 0)  AS "current_stock",

          -- 적정재고: Avrqty 사용
          m.avrqty_num                  AS "optimal_stock",

          -- 차이재고
          (COALESCE(s.current_stock,0) - m.avrqty_num) AS "diff_stock",

          -- 상태
          CASE
            WHEN COALESCE(s.current_stock,0) > m.avrqty_num THEN '과잉'
            WHEN COALESCE(s.current_stock,0) = m.avrqty_num THEN '적정'
            ELSE '부족'
          END AS "state",

          r.last_in_date                AS "last_in_date",
          r.house_name,
          otr.order_type_resolved       AS "order_type_used" -- 표시용

        FROM mat m
        LEFT JOIN stock s                 ON s.mat_id = m.id
        LEFT JOIN last_receipt r          ON r.mat_id = m.id
        LEFT JOIN order_type_resolved otr ON otr.id   = m.id
        LEFT JOIN "unit" u                ON u.id     = m."Unit_id"

        WHERE m.spjangcd = :spjangcd
          -- 입고 이력이 없는 자재도 포함하려면 NULL 허용
          AND (r."last_in_date" BETWEEN :start AND :end OR r."last_in_date" IS NOT NULL)
        """;

    // 품명 필터 (대소문자 구분 없이 검색 원하면 ILIKE 권장: Postgres)
    if (matName != null && !matName.isEmpty()) {
      sql += " AND m.name ILIKE :matName ";
      paramMap.addValue("matName", '%' + matName + '%');
    }

    // 창고 필터: last_receipt를 통해 필터링(입고 이력 없는 행은 제외됨에 유의)
    if (store_id != null) {
      sql += " AND r.store_house_id = :store_id ";
      paramMap.addValue("store_id", store_id);
    }

    // 상태 필터 (Avrqty 기준)
    if (status != null && !status.isBlank() && !"전체".equals(status)) {
      status = status.trim().toLowerCase();
      switch (status) {
        case "excess": status = "과잉"; break;
        case "proper": status = "적정"; break;
        case "tribe":  status = "부족"; break;
        // 한글 그대로 들어오면 그대로 사용
      }
      sql += """
          AND (
            CASE
              WHEN COALESCE(s.current_stock,0) > m.avrqty_num THEN '과잉'
              WHEN COALESCE(s.current_stock,0) = m.avrqty_num THEN '적정'
              ELSE '부족'
            END
          ) = :status
        """;
      paramMap.addValue("status", status);
    }

    sql += " ORDER BY m.code ";

//    log.info("paramMap:{}", paramMap);
//    log.info("자재 적정재고(Avrqty) 현황 sql:{}", sql);
    return sqlRunner.getRows(sql, paramMap);
  }

}
