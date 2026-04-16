package mes.app.system.service;

import io.micrometer.core.instrument.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import mes.app.common.TenantContext;
import mes.app.files.NcpObjectStorageService;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class NoticeService {

    @Autowired
    SqlRunner sqlRunner;

    @Autowired
    NcpObjectStorageService storageService;

    private static final DateTimeFormatter YYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd");

    // 공지사항 목록 조회
    public List<Map<String, Object>> getBoardList(String keyword, String srchStartDt, String srchEndDt) {

        String spjangcd = TenantContext.get();
        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("srchStartDt", srchStartDt);
        paramMap.addValue("srchEndDt", srchEndDt);
        paramMap.addValue("keyword", keyword);
        paramMap.addValue("spjangcd", spjangcd);

        String sql = """
			select BBSSEQ as id
			, BBSSUBJECT as title
			, BBSDATE as write_date_time
			, BBSFRDATE as notice_from_date
			, BBSTODATE as notice_end_date
			, notice_yn
			from tb_bbsinfo
			where BBSDATE between :srchStartDt and :srchEndDt
			and spjangcd = :spjangcd
			""";

        if (!StringUtils.isEmpty(keyword)) {
            sql += """
                and (BBSSUBJECT like '%' || :keyword || '%'
                    or BBSTEXT like '%' || :keyword || '%')
                    """;
        }

        sql += " order by BBSSEQ desc ";

        return this.sqlRunner.getRows(sql, paramMap);
    }

    // 공지사항 상세 조회
    public Map<String, Object> getBoardDetail(Integer bbsseq) {
        String spjangcd = TenantContext.get();
        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("bbsseq", bbsseq);
        paramMap.addValue("spjangcd", spjangcd);

        String sql = """
                select BBSSEQ as id
                , BBSSUBJECT as title
                , BBSTEXT as content
                , BBSFRDATE as notice_from_date
                , BBSTODATE as notice_end_date
                , notice_yn
                from tb_bbsinfo
                where BBSSEQ = :bbsseq
                and spjangcd = :spjangcd
                """;

        return this.sqlRunner.getRow(sql, paramMap);
    }

    // 공지사항 저장 (신규/수정), 저장된 BBSSEQ 반환
    public Integer saveNotice(Integer bbsseq, String subject, String text,
                              String noticeYn, String noticeFromDate, String noticeEndDate, String userId) {

        String today = LocalDate.now().format(YYYYMMDD);

        String bbsfrdate = (noticeFromDate != null && !noticeFromDate.isEmpty()) ? noticeFromDate.replace("-", "") : null;
        String bbstodate = (noticeEndDate != null && !noticeEndDate.isEmpty()) ? noticeEndDate.replace("-", "") : null;
        String spjangcd = TenantContext.get();
        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("bbssubject", subject);
        paramMap.addValue("bbstext", text);
        paramMap.addValue("bbsfrdate", bbsfrdate);
        paramMap.addValue("bbstodate", bbstodate);
        paramMap.addValue("noticeYn", noticeYn != null ? noticeYn : "N");
        paramMap.addValue("userid", userId);
        paramMap.addValue("spjangcd", spjangcd);

        if (bbsseq == null) {
            paramMap.addValue("bbsdate", today);
            String sql = """
                INSERT INTO tb_bbsinfo
                    (bbsdate, bbssubject, bbsuser, bbstext, bbsfrdate, bbstodate, notice_yn, indatem, inuserid, spjangcd)
                VALUES
                    (:bbsdate, :bbssubject, :userid, :bbstext, :bbsfrdate, :bbstodate, :noticeYn, NOW(), :userid, :spjangcd)
                RETURNING bbsseq
                """;
            Map<String, Object> row = this.sqlRunner.getRow(sql, paramMap);
            if (row == null) return null;
            return ((Number) row.get("BBSSEQ")).intValue();
        } else {
            paramMap.addValue("bbsseq", bbsseq);
            String sql = """
                update tb_bbsinfo
                set BBSSUBJECT = :bbssubject
                , BBSTEXT     = :bbstext
                , BBSFRDATE   = :bbsfrdate
                , BBSTODATE   = :bbstodate
                , notice_yn   = :noticeYn
                where BBSSEQ = :bbsseq
                """;
            int affected = this.sqlRunner.execute(sql, paramMap);
            return affected > 0 ? bbsseq : null;
        }
    }

    // 현재 활성 공지 조회 (index 팝업용)
    public List<Map<String, Object>> getActiveNotices() {
        String today = LocalDate.now().format(YYYYMMDD);
        String spjangcd = TenantContext.get();
        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("today", today);
        paramMap.addValue("spjangcd", spjangcd);

        String sql = """
                select BBSSEQ as id
                , BBSSUBJECT as title
                , BBSTEXT as content
                , BBSFRDATE as notice_from_date
                , BBSTODATE as notice_end_date
                , notice_yn
                from tb_bbsinfo
                where notice_yn = 'Y'
                and BBSFRDATE <= :today
                and BBSTODATE >= :today
                and spjangcd = :spjangcd
                """;
        return this.sqlRunner.getRows(sql, paramMap);
    }

    // 공지사항 삭제 (NCP 물리 파일 + TB_FILEINFO + TB_BBSINFO)
    public void deleteNotice(Integer bbsseq) {
        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("bbsseq", bbsseq);

        // 1. NCP 물리 파일 삭제 (TB_FILEINFO 삭제 전에 먼저 조회)
        List<Map<String, Object>> files = this.sqlRunner.getRows(
                "select FILEPATH, FILESVNM from TB_FILEINFO where bbsseq = :bbsseq", paramMap);
        for (Map<String, Object> file : files) {
            try {
                String objectKey = file.get("FILEPATH") + "/" + file.get("FILESVNM");
                storageService.delete(objectKey);
            } catch (Exception e) {
                log.error("[deleteNotice] NCP 파일 삭제 오류 (bbsseq={}): {}", bbsseq, e.getMessage(), e);
            }
        }

        // 2. TB_FILEINFO 레코드 삭제
        this.sqlRunner.execute("delete from tb_fileinfo where bbsseq = :bbsseq", paramMap);

        // 3. TB_BBSINFO 레코드 삭제
        this.sqlRunner.execute("delete from tb_bbsinfo where BBSSEQ = :bbsseq", paramMap);
    }
}
