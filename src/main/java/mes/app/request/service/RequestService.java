package mes.app.request.service;

import mes.app.common.TenantContext;
import mes.domain.entity.TbAs010;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.TbAs010Repository;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Service
public class RequestService {
    @Autowired
    SqlRunner sqlRunner;

    @Autowired
    TbAs010Repository tbAs010Repository;

    // 거래처 정보 조회
    public Map<String, Object> searchUserInfo(
            String compid
    ) {
        String tenantId = TenantContext.get();
        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("compid", compid);
        dicParam.addValue("spjangcd", tenantId);
        String sql = """
                SELECT
                    *
                FROM company
                WHERE "BusinessNumber"=:compid
                and spjangcd=:spjangcd
        		""";


        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

        return item;
    }

    // 사용자 거래처 직원 유무 조회
    public Map<String, Object> boolUserInfo(String userid) {
        String tenantId = TenantContext.get();
        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("userid", userid);
        dicParam.addValue("spjangcd", tenantId);
        String sql = """
                SELECT
                    *
                FROM user_profile
                WHERE "User_id"= (select id from auth_user where username = :userid)
                and spjangcd = :spjangcd
        		""";


        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

        return item;
    }
    // 요청사항 조회
    public List<Map<String, Object>> searchDatas(
            String searchfrdate
            , String searchtodate
            , String searchCompCd
            , String reqType
            , String recyn
            , String usernm
            , String spjangcd
    ) {
        // 날짜 형식 변환 (YYYY-MM-DD -> YYYYMMDD)
        if (searchfrdate != null && searchfrdate.contains("-")) {
            searchfrdate = searchfrdate.replaceAll("-", "");
        }
        if (searchtodate != null && searchtodate.contains("-")) {
            searchtodate = searchtodate.replaceAll("-", "");
        }

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("searchfrdate", searchfrdate);
        dicParam.addValue("searchtodate", searchtodate);
        dicParam.addValue("searchCompCd", searchCompCd);
        dicParam.addValue("reqType", reqType);
        dicParam.addValue("spjangcd", spjangcd);

        String sql = """
                SELECT
                    a."asid" AS id,
                    TO_CHAR(TO_DATE(a."asdate", 'YYYYMMDD'), 'YYYY-MM-DD') AS asdate,
                    a."cltnm",
                    a."cltcd",
                    a."userid",
                    a."usernm",
                    a."asperid",
                    a."aspernm",
                    a."retitle",
                    a."remark" AS content,
                    a."asdv",
                    sc1."Value" AS asdv_nm,
                    a."asmenu",
                    a."recyn",
                    sc2."Value" AS recyn_nm,
                    a."recperid",
                    a."recpernm",
                    a."recdate",
                    a."endperid",
                    a."endpernm",
                    f."as_file" as "fix_file",
                    f."remark",
                    TO_CHAR(TO_DATE(a."enddate", 'YYYYMMDD'), 'YYYY-MM-DD') AS enddate,
                    TO_CHAR(a."inputdate", 'YYYY-MM-DD HH24:MI') AS inputdate
                FROM "tb_as010" a
                LEFT JOIN "sys_code" sc1
                    ON sc1."Code" = a."asdv"
                   AND sc1."CodeType" = 'asdv'
                LEFT JOIN "sys_code" sc2
                    ON sc2."Code" = a."recyn"
                   AND sc2."CodeType" = 'recyn'
                LEFT JOIN "tb_as011" f
                    ON f."asid" = a."asid"
                WHERE 1=1
        		""";

        // 날짜 조건 추가
        if (searchfrdate != null && !searchfrdate.isEmpty()) {
            sql += " AND a.\"asdate\" >= :searchfrdate ";
        }
        if (searchtodate != null && !searchtodate.isEmpty()) {
            sql += " AND a.\"asdate\" <= :searchtodate ";
        }

        // 업체 조건 추가
        if (searchCompCd != null && !searchCompCd.isEmpty()) {
            sql += " AND a.\"cltcd\" = :searchCompCd ";
        }

        // 요청구분 조건 추가
        if (reqType != null && !reqType.isEmpty()) {
            sql += " AND a.\"asdv\" = :reqType ";
        }
        // 요청자 조건 추가
        if (usernm != null && !usernm.isEmpty()) {
            dicParam.addValue("usernm", usernm);
            sql += " AND a.\"usernm\" = :usernm ";
        }
        // 진행구분 조건 추가
        if (recyn != null && !recyn.isEmpty()) {
            dicParam.addValue("recyn", recyn);
            sql += " AND a.\"recyn\" = :recyn ";
        }
        sql += " ORDER BY a.\"asdate\" DESC, a.\"inputdate\" DESC ";

        List<Map<String, Object>> item = this.sqlRunner.getRows(sql, dicParam);

        return item;
    }

    // 상세정보 조회
    public Map<String, Object> getDetail(Integer id) {
        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("id", id);

        String sql = """
        SELECT
            a."asid" AS id,
            TO_CHAR(TO_DATE(a."asdate", 'YYYYMMDD'), 'YYYY-MM-DD') AS "reqDate",
            a."cltnm",
            a."cltcd" AS "Code",
            a."userid",
            a."usernm" AS reqPer,
            a."asperid",
            a."aspernm" AS OurManager,
            a."retitle" AS title,
            a."remark" AS content,
            a."asdv" AS reqType,
            sc1."Value" AS reqType_nm,
            a."asmenu" AS scrNum,
            a."recyn",
            sc2."Value" AS recyn_nm,
            a."recperid",
            a."recpernm",
            a."recdate",
            a."endperid",
            a."endpernm",
            TO_CHAR(TO_DATE(a."enddate", 'YYYYMMDD'), 'YYYY-MM-DD') AS endDate,
            TO_CHAR(a."inputdate", 'YYYY-MM-DD HH24:MI') AS inputdate,
            a."as_file" AS as_file,
            c."spjangcd" AS spjangcd,
            c."TelNumber",
            -- ⚙️ company id가 필요할 경우 join
            c.id AS cboCompanyHidden,
            c."BusinessNumber" AS spNum
        FROM "tb_as010" a
        LEFT JOIN "company" c
            ON c."Code" = a."cltcd"
        LEFT JOIN "sys_code" sc1
            ON sc1."Code" = a."asdv"
           AND sc1."CodeType" = 'sale_type'
        LEFT JOIN "sys_code" sc2
            ON sc2."Code" = a."recyn"
           AND sc2."CodeType" = 'recyn'
        WHERE a."asid" = :id
        """;

        Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
        return item;
    }


    // 저장
    @Transactional
    public AjaxResult saveRequest(Map<String, Object> payload, User user) {
        AjaxResult result = new AjaxResult();

        try {
            // id 체크
            Integer id = payload.get("id") != null && !payload.get("id").toString().isEmpty()
                    ? Integer.parseInt(payload.get("id").toString())
                    : null;

            TbAs010 entity = null;

            // 수정 or 신규 등록
            if (id != null) {
                entity = tbAs010Repository.findById(id)
                        .orElseThrow(() -> new RuntimeException("데이터를 찾을 수 없습니다."));
            } else {
                entity = new TbAs010();
                entity.setInputdate(new Timestamp(System.currentTimeMillis()));
                entity.setUserid(String.valueOf(payload.get("spNum")));
                entity.setUsernm(user.getUsername());
            }

            // 요청일자
            String reqDate = cleanDate(payload.get("reqDate"));
            if (reqDate != null) {
                entity.setAsdate(reqDate);
            }

            // 업체명 및 코드 설정
            Object cboCompanyHiddenObj = payload.get("cboCompanyHidden");
            if (cboCompanyHiddenObj != null && !cboCompanyHiddenObj.toString().isEmpty()) {
                try {
                    String compId = null;
                    if (cboCompanyHiddenObj instanceof Integer) {
                        compId = (String) cboCompanyHiddenObj;
                    } else if (cboCompanyHiddenObj instanceof String) {
                        compId = (String) cboCompanyHiddenObj;
                    }

                    if (compId != null) {
                        MapSqlParameterSource paramMap = new MapSqlParameterSource();
                        paramMap.addValue("compId", compId);
                        String compSql = "SELECT \"Name\", \"Code\" FROM company WHERE \"Code\" = :compId";
                        Map<String, Object> compData = this.sqlRunner.getRow(compSql, paramMap);
                        if (compData != null) {
                            if (compData.get("Name") != null)
                                entity.setCltnm(compData.get("Name").toString());
                            if (compData.get("Code") != null)
                                entity.setCltcd(compData.get("Code").toString());
                        }
                    }
                } catch (Exception e) {
                    // 업체 조회 실패 시 무시
                }
            }

            // 요청자
            if (payload.get("reqPer") != null) {
                entity.setUsernm(payload.get("reqPer").toString());
            }

            // 제목
            if (payload.get("title") != null) {
                entity.setRetitle(payload.get("title").toString());
            }

            // 요청구분
            if (payload.get("reqType") != null) {
                entity.setAsdv(payload.get("reqType").toString());
            }

            // 화면명
            if (payload.get("scrNum") != null) {
                entity.setAsmenu(payload.get("scrNum").toString());
            }

            // 요청내용
            if (payload.get("content") != null) {
                entity.setRemark(payload.get("content").toString());
            }

            // 파일명 저장 (기존 파일이 있을 경우 교체 시 이전 파일 삭제)
            if (payload.get("as_file") != null) {
                String newFileName = payload.get("as_file").toString();
                String oldFileName = entity.getAsFile();

                if (oldFileName != null && !oldFileName.isEmpty() && !oldFileName.equals(newFileName)) {
                    try {
                        Path oldPath = Paths.get("C:/temp/as_request/files/" + oldFileName);
                        Files.deleteIfExists(oldPath);
                    } catch (Exception e) {
                        // 파일 삭제 실패는 업무 로직에 치명적이지 않으므로 무시
                    }
                }

                entity.setAsFile(newFileName);
            }

            // ✅ OurManager → person 테이블 매핑 추가
            if (payload.get("OurManager") != null && !payload.get("OurManager").toString().isEmpty()) {
                String managerName = payload.get("OurManager").toString();

                MapSqlParameterSource personParam = new MapSqlParameterSource();
                personParam.addValue("name", managerName);

                String personSql = """
                    SELECT id, "Name"
                    FROM person
                    WHERE "Name" = :name
                    """;

                Map<String, Object> personData = this.sqlRunner.getRow(personSql, personParam);
                if (personData != null) {
                    Object personId = personData.get("id");
                    Object personName = personData.get("Name");

                    if (personId != null) {
                        entity.setAsperid(String.valueOf(Integer.parseInt(personId.toString()))); // ✅ asperid 설정
                    }
                    if (personName != null) {
                        entity.setAspernm(personName.toString()); // ✅ aspernm 설정
                    }
                }
            }

            // 수정일자
            entity.setInputdate(new Timestamp(System.currentTimeMillis()));

            tbAs010Repository.save(entity);

            result.success = true;
            result.data = entity.getAsid();

        } catch (Exception e) {
            e.printStackTrace();
            result.success = false;
            result.message = e.getMessage();
        }

        return result;
    }


    private String cleanDate(Object v) {
        if (v == null) return null;
        String dateStr = v.toString();
        return dateStr.replaceAll("-", "");
    }

    // 거래처 정보 조회
    public List<Map<String, Object>> getComp(
            String searchCode
            , String searchName
            , String spjangcd
    ) {
        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("spjangcd", spjangcd);

        StringBuilder sql = new StringBuilder("""
            SELECT
                *
            FROM company
            WHERE 1=1
            AND spjangcd = :spjangcd
            """);

        if (searchCode != null && !searchCode.isEmpty()) {
            sql.append(" AND \"Code\" LIKE :searchCode");
            dicParam.addValue("searchCode", "%" + searchCode + "%");
        }

        if (searchName != null && !searchName.isEmpty()) {
            sql.append(" AND \"Name\" LIKE :searchName");
            dicParam.addValue("searchName", "%" + searchName + "%");
        }

        sql.append(" ORDER BY \"Code\" ASC");

        List<Map<String, Object>> item = this.sqlRunner.getRows(String.valueOf(sql), dicParam);

        return item;
    }

    // 사용자(본사담당) 정보 조회
    public List<Map<String, Object>> getUser(
            String searchCode
            , String searchName
            , String spjangcd
    ) {
        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("spjangcd", spjangcd);

        StringBuilder sql = new StringBuilder("""
            SELECT
                up.*,
                dp.\"Name\" as \"depName\"
            FROM user_profile up
            LEFT JOIN depart dp ON up.\"Depart_id\" = dp.id
            LEFT JOIN auth_user au ON up.\"User_id\" = au.id
            LEFT JOIN person p ON au.\"personid\" = p.id
            WHERE 1=1
            AND up.spjangcd = :spjangcd
            """);

        if (searchCode != null && !searchCode.isEmpty()) {
            sql.append(" AND up.\"Depart_id\" = :searchCode");
            dicParam.addValue("searchCode", searchCode );
        }

        if (searchName != null && !searchName.isEmpty()) {
            sql.append(" AND up.\"Name\" LIKE :searchName");
            dicParam.addValue("searchName", "%" + searchName + "%");
        }

        sql.append(" AND p.\"rtflag\" = '0' ");
        sql.append(" AND up.\"UserGroup_id\" = 2 ");
        sql.append(" ORDER BY up.\"User_id\" ASC");

        List<Map<String, Object>> item = this.sqlRunner.getRows(String.valueOf(sql), dicParam);

        return item;
    }
}
