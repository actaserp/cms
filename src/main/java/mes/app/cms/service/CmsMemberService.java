package mes.app.cms.service;

import lombok.extern.slf4j.Slf4j;
import mes.app.common.TenantContext;
import mes.app.files.NcpObjectStorageService;
import mes.domain.services.SqlRunner;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Service
public class CmsMemberService {

    @Autowired
    SqlRunner sqlRunner;

    @Autowired
    private CmsAccountRegisterService cmsAccountRegisterService;

    @Autowired
    private NcpObjectStorageService storageService;

    @Autowired
    private CmsEb13SendService cmsEb13SendService;

    private String str(Object v) {
        return v != null ? v.toString() : "";
    }

    private static final java.util.concurrent.atomic.AtomicInteger TRACKING_SEQ =
            new java.util.concurrent.atomic.AtomicInteger(0);

    /**
     * 납부자 목록 조회
     */
    public List<Map<String, Object>> getMemberList(String memberName, String memberNo, String status) {
        return getMemberList(memberName, memberNo, null, status);
    }

    public List<Map<String, Object>> getMemberList(String memberName, String memberNo, String keyword, String status) {
        String spjangcd = TenantContext.get();
        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);

        String sql = """
                SELECT m.id
                     , m.member_type
                     , CASE m.member_type WHEN 'C' THEN '법인' WHEN 'S' THEN '개인사업자' ELSE '개인' END AS member_type_name
                     , m.member_name
                     , m.member_no
                     , m.id_number
                     , m.resident_no
                     , m.biz_no
                     , m.phone
                     , m.email
                     , m.zipcd
                     , m.adresa
                     , m.adresb
                     , m.bank_code
                     , b.bank_name
                     , m.bank_account
                     , m.account_holder
                     , m.deduct_day
                     , m.deduct_amount
                     , m.cycle_type
                     , m.cycle_months
                     , m.start_date
                     , m.end_date
                     , m.pause_start_date
                     , m.pause_end_date
                     , m.pause_reason
                     , m.agree_yn
                     , m.agree_date
                     , m.agree_method
                     , m.status
                     , m.memo
                     , m._created
                     , m._modified
                     , CASE
                           WHEN m.agree_yn = 'Y'        THEN '인증완료'
                           WHEN r.status = 'REJECTED'   THEN '인증거절'
                           WHEN r.status IN ('PENDING')  THEN '인증대기'
                           WHEN r.status = 'FAILED'     THEN '인증실패'
                           ELSE '미신청'
                       END AS agree_status
                FROM cms_member m
                LEFT JOIN cms_bank_code b ON b.bank_code = m.bank_code
                LEFT JOIN LATERAL (
                    SELECT status FROM cms_account_register
                    WHERE member_id = m.id AND spjangcd = m.spjangcd
                    ORDER BY _created DESC LIMIT 1
                ) r ON true
                WHERE m.spjangcd = :spjangcd
                """;

        if (StringUtils.hasText(memberName)) {
            sql += " AND m.member_name LIKE '%' || :memberName || '%'";
            param.addValue("memberName", memberName);
        }
        if (StringUtils.hasText(memberNo)) {
            sql += " AND m.member_no LIKE '%' || :memberNo || '%'";
            param.addValue("memberNo", memberNo);
        }
        // 통합검색: 이름 OR 번호 (계좌조회 모달 검색용)
        if (StringUtils.hasText(keyword)) {
            sql += " AND (m.member_name LIKE '%' || :keyword || '%'"
                    + "      OR m.member_no LIKE '%' || :keyword || '%')";
            param.addValue("keyword", keyword);
        }
        if (StringUtils.hasText(status)) {
            sql += " AND m.status = :status";
            param.addValue("status", status);
        }

        sql += " ORDER BY m.id DESC";
        return sqlRunner.getRows(sql, param);
    }

    /**
     * 납부자 단건 조회
     */
    public Map<String, Object> getMember(Long id) {
        String spjangcd = TenantContext.get();
        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("id", id);
        param.addValue("spjangcd", spjangcd);

        String sql = """
                SELECT m.id
                     , m.member_type
                     , m.member_name
                     , m.member_no
                     , m.id_number
                     , m.resident_no
                     , m.biz_no
                     , m.phone
                     , m.email
                     , m.zipcd
                     , m.adresa
                     , m.adresb
                     , m.bank_code
                     , b.bank_name
                     , m.bank_account
                     , m.account_holder
                     , m.deduct_day
                     , m.deduct_amount
                     , m.cycle_type
                     , m.cycle_months
                     , m.deduct_month_type
                     , m.start_date
                     , m.end_date
                     , m.pause_start_date
                     , m.pause_end_date
                     , m.pause_reason
                     , m.agree_yn
                     , m.agree_date
                     , m.agree_method
                     , m.status
                     , m.memo
                FROM cms_member m
                LEFT JOIN cms_bank_code b ON b.bank_code = m.bank_code
                WHERE m.id = :id AND m.spjangcd = :spjangcd
                """;
        return sqlRunner.getRow(sql, param);
    }

    /**
     * 납부자 저장 (신규/수정)
     */
    public Long saveMember(Long id, String memberType, String memberName, String memberNo,
                           String idNumber, String residentNo, String bizNo, String phone, String email,
                           String zipcd, String adresa, String adresb,
                           String bankCode, String bankAccount, String accountHolder,
                           String deductDay, Long deductAmount,
                           String cycleType, String cycleMonths,
                           String deductMonthType,
                           String startDate, String endDate,
                           String pauseStartDate, String pauseEndDate, String pauseReason,  // 추가됨
                           String agreeYn, String agreeMethod,
                           String status, String memo,
                           String userId) {

        String spjangcd = TenantContext.get();

        // 중지 기간 유효성 검사
        if (StringUtils.hasText(pauseStartDate) && StringUtils.hasText(pauseEndDate)) {
            validatePausePeriod(pauseStartDate, pauseEndDate);
        } else if (StringUtils.hasText(pauseStartDate) || StringUtils.hasText(pauseEndDate)) {
            // 시작일과 종료일 중 하나만 입력된 경우
            throw new IllegalArgumentException("중지 기간은 시작일과 종료일을 모두 입력하거나 비워주세요.");
        }

        // 신규 등록
        if (id == null) {
            if (!StringUtils.hasText(memberNo)) {
                memberNo = generateMemberNo(spjangcd, bankCode, idNumber);
            } else {
                memberNo = memberNo.toUpperCase();
            }

            MapSqlParameterSource param = new MapSqlParameterSource();
            param.addValue("spjangcd",      spjangcd);
            param.addValue("memberType",    StringUtils.hasText(memberType) ? memberType : "C");
            param.addValue("memberName",    memberName);
            param.addValue("memberNo",      memberNo);
            param.addValue("idNumber",      idNumber);
            param.addValue("residentNo",    residentNo);
            param.addValue("bizNo",         bizNo);
            param.addValue("phone",         phone);
            param.addValue("email",         email);
            param.addValue("zipcd",         zipcd);
            param.addValue("adresa",        adresa);
            param.addValue("adresb",        adresb);
            param.addValue("bankCode",      bankCode);
            param.addValue("bankAccount",   bankAccount);
            param.addValue("accountHolder", accountHolder);
            param.addValue("deductDay",     deductDay);
            param.addValue("deductAmount",  deductAmount);
            param.addValue("cycleType",     cycleType);
            param.addValue("cycleMonths",   cycleMonths);
            param.addValue("deductMonthType", org.springframework.util.StringUtils.hasText(deductMonthType) ? deductMonthType : "CURRENT");
            param.addValue("startDate",     startDate);
            param.addValue("endDate",       endDate);
            param.addValue("pauseStartDate",  pauseStartDate);  // 추가됨
            param.addValue("pauseEndDate",    pauseEndDate);    // 추가됨
            param.addValue("pauseReason",     pauseReason);     // 추가됨
            param.addValue("agreeYn",       agreeYn);
            param.addValue("agreeMethod",   agreeMethod);
            param.addValue("status",        StringUtils.hasText(status) ? status : "ACTIVE");
            param.addValue("memo",          memo);
            param.addValue("userId",        userId);

            String insertSql = """
                    INSERT INTO cms_member (
                        spjangcd, member_type, member_name, member_no,
                        id_number, resident_no, biz_no, phone, email,
                        zipcd, adresa, adresb,
                        bank_code, bank_account, account_holder,
                        deduct_day, deduct_amount,
                        cycle_type, cycle_months,
                        deduct_month_type,
                        start_date, end_date,
                        pause_start_date, pause_end_date, pause_reason,
                        agree_yn, agree_method, status, memo,
                        _creater_id, _created, _modifier_id, _modified
                    ) VALUES (
                        :spjangcd, :memberType, :memberName, :memberNo,
                        :idNumber, :residentNo, :bizNo, :phone, :email,
                        :zipcd, :adresa, :adresb,
                        :bankCode, :bankAccount, :accountHolder,
                        :deductDay, :deductAmount,
                        :cycleType, :cycleMonths,
                        :deductMonthType,
                        :startDate, :endDate,
                        :pauseStartDate, :pauseEndDate, :pauseReason,
                        :agreeYn, :agreeMethod, :status, :memo,
                        :userId, NOW(), :userId, NOW()
                    )
                    """;

            sqlRunner.execute(insertSql, param);

            // 신규 등록 로그
            log.info("[CMS] 납부자 신규 등록 - ID: {}, 이름: {}, 중지기간: {} ~ {}",
                    memberNo, memberName, pauseStartDate, pauseEndDate);

            Map<String, Object> result = sqlRunner.getRow(
                    "SELECT id FROM cms_member WHERE spjangcd = :spjangcd AND member_no = :memberNo",
                    new MapSqlParameterSource("spjangcd", spjangcd).addValue("memberNo", memberNo));

            Long savedId = result != null ? ((Number) result.get("id")).longValue() : null;
            if (savedId != null && StringUtils.hasText(bankAccount)) {
                cmsAccountRegisterService.save(savedId, "1", null, null, userId);
            }

            return result != null ? ((Number) result.get("id")).longValue() : null;
        }

        // 수정
        else {
            MapSqlParameterSource param = new MapSqlParameterSource();
            param.addValue("id",            id);
            param.addValue("memberNo", memberNo);
            param.addValue("memberType",    StringUtils.hasText(memberType) ? memberType : "C");
            param.addValue("memberName",    memberName);
            param.addValue("idNumber",      idNumber);
            param.addValue("residentNo",    residentNo);
            param.addValue("bizNo",         bizNo);
            param.addValue("phone",         phone);
            param.addValue("email",         email);
            param.addValue("zipcd",         zipcd);
            param.addValue("adresa",        adresa);
            param.addValue("adresb",        adresb);
            param.addValue("bankCode",      bankCode);
            param.addValue("bankAccount",   bankAccount);
            param.addValue("accountHolder", accountHolder);
            param.addValue("deductDay",     deductDay);
            param.addValue("deductAmount",  deductAmount);
            param.addValue("cycleType",     cycleType);
            param.addValue("cycleMonths",   cycleMonths);
            param.addValue("deductMonthType", org.springframework.util.StringUtils.hasText(deductMonthType) ? deductMonthType : "CURRENT");
            param.addValue("startDate",     startDate);
            param.addValue("endDate",       endDate);
            param.addValue("pauseStartDate",  pauseStartDate);  // 추가됨
            param.addValue("pauseEndDate",    pauseEndDate);    // 추가됨
            param.addValue("pauseReason",     pauseReason);     // 추가됨
            param.addValue("agreeYn",       agreeYn);
            param.addValue("agreeMethod",   agreeMethod);
            param.addValue("status",        status);
            param.addValue("memo",          memo);
            param.addValue("userId",        userId);
            param.addValue("spjangcd",      spjangcd);

            String updateSql = """
                    UPDATE cms_member SET
                        member_no      = :memberNo,
                        member_type    = :memberType,
                        member_name    = :memberName,
                        id_number      = :idNumber,
                        resident_no    = :residentNo,
                        biz_no         = :bizNo,
                        phone          = :phone,
                        email          = :email,
                        zipcd          = :zipcd,
                        adresa         = :adresa,
                        adresb         = :adresb,
                        bank_code      = :bankCode,
                        bank_account   = :bankAccount,
                        account_holder = :accountHolder,
                        deduct_day     = :deductDay,
                        deduct_amount  = :deductAmount,
                        cycle_type     = :cycleType,
                        cycle_months   = :cycleMonths,
                        deduct_month_type = :deductMonthType,
                        start_date     = :startDate,
                        end_date       = :endDate,
                        pause_start_date = :pauseStartDate,
                        pause_end_date   = :pauseEndDate,
                        pause_reason     = :pauseReason,
                        agree_yn       = :agreeYn,
                        agree_method   = :agreeMethod,
                        status         = :status,
                        memo           = :memo,
                        _modifier_id   = :userId,
                        _modified      = NOW()
                    WHERE id = :id AND spjangcd = :spjangcd
                    """;

            sqlRunner.execute(updateSql, param);

            // 납부자 정보 변경분을 신청행에 반영.
            // 화면 isSendable()과 같은 규칙 — "다시 보낼 수 있는 행"만 갱신한다.
            //  · APPROVED/CANCELLED : 확정된 이력이므로 불변
            //  · eb13 SENT + EB14 미수신 : 금결원 계류 중, 결과 매칭 기준이라 불변
            //  · PENDING / FAILED / REJECTED : 재전송 대상 → 갱신
            sqlRunner.execute("""
                UPDATE cms_account_register
                SET member_no      = :memberNo,
                    member_name    = :memberName,
                    bank_code      = :bankCode,
                    bank_account   = :bankAccount,
                    account_holder = :accountHolder,
                    id_number      = :idNumber,
                    member_type    = :memberType,
                    _modified      = NOW()
                WHERE member_id = :id
                  AND spjangcd  = :spjangcd
                  AND COALESCE(status,'') NOT IN ('APPROVED', 'CANCELLED')
                  AND NOT (eb13_status = 'SENT' AND eb14_received_at IS NULL)
                """, param);

            return id;
        }
    }

    /**
     * 중지 기간 유효성 검사 (추가됨)
     */
    private void validatePausePeriod(String pauseStartDate, String pauseEndDate) {
        try {
            LocalDate start = LocalDate.parse(pauseStartDate, DateTimeFormatter.ofPattern("yyyyMMdd"));
            LocalDate end = LocalDate.parse(pauseEndDate, DateTimeFormatter.ofPattern("yyyyMMdd"));

            if (start.isAfter(end)) {
                throw new IllegalArgumentException("중지 종료일은 시작일보다 같거나 이후여야 합니다.");
            }

            // 과거 날짜 경고 (로그에만 남김)
            LocalDate today = LocalDate.now();
            if (end.isBefore(today)) {
                log.warn("[CMS] 이미 지난 중지 기간이 설정되었습니다 - 시작: {}, 종료: {}",
                        pauseStartDate, pauseEndDate);
            }
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("날짜 형식이 올바르지 않습니다. YYYYMMDD 형식으로 입력해주세요.");
        }
    }

    /**
     * 특정 날짜가 중지 기간인지 확인 (청구 생성 시 사용)
     */
    public boolean isPausedOnDate(Long memberId, LocalDate date) {
        Map<String, Object> member = getMember(memberId);
        if (member == null) return false;

        String pauseStartDate = str(member.get("pause_start_date"));
        String pauseEndDate = str(member.get("pause_end_date"));

        if (!StringUtils.hasText(pauseStartDate) || !StringUtils.hasText(pauseEndDate)) {
            return false;
        }

        try {
            LocalDate start = LocalDate.parse(pauseStartDate, DateTimeFormatter.ofPattern("yyyyMMdd"));
            LocalDate end = LocalDate.parse(pauseEndDate, DateTimeFormatter.ofPattern("yyyyMMdd"));

            return !date.isBefore(start) && !date.isAfter(end);
        } catch (Exception e) {
            log.error("[CMS] 중지 기간 확인 중 오류 - memberId: {}, date: {}", memberId, date, e);
            return false;
        }
    }

    /**
     * 납부자 삭제 (soft delete)
     */
    public boolean deleteMember(Long id) {
        String spjangcd = TenantContext.get();
        var param = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("spjangcd", spjangcd);

        // 1. 매핑 테이블
        sqlRunner.execute("DELETE FROM cms_file_billing WHERE billing_id IN (SELECT id FROM cms_billing WHERE member_id = :id AND spjangcd = :spjangcd)", param);
        sqlRunner.execute("DELETE FROM cms_file_register WHERE register_id IN (SELECT id FROM cms_account_register WHERE member_id = :id AND spjangcd = :spjangcd)", param);

        // 2. 본 테이블
        sqlRunner.execute("DELETE FROM cms_billing WHERE member_id = :id AND spjangcd = :spjangcd", param);
        sqlRunner.execute("DELETE FROM cms_account_register WHERE member_id = :id AND spjangcd = :spjangcd", param);

        // 3. cms_file — 해당 member 관련 파일 (다른 member 파일과 공유될 수 있으니 고아 파일만)
        // cms_file은 여러 member가 공유할 수 있어서 file_register/file_billing 없는 것만 삭제
        sqlRunner.execute("""
    DELETE FROM cms_file 
    WHERE spjangcd = :spjangcd
      AND id NOT IN (SELECT file_id FROM cms_file_billing)
      AND id NOT IN (SELECT file_id FROM cms_file_register)
    """, param);

        // 4. 마지막
        return sqlRunner.execute("DELETE FROM cms_member WHERE id = :id AND spjangcd = :spjangcd", param) > 0;
    }

    public Map<String, Object> excelUpload(MultipartFile file, String userId) {
        String spjangcd = TenantContext.get();
        int inserted = 0, updated = 0, failed = 0;

        // 은행명 → 코드 맵 로드
        List<Map<String, Object>> bankList = sqlRunner.getRows(/* skip_tenant_check */
                "SELECT bank_code, bank_name FROM cms_bank_code",
                new MapSqlParameterSource());
        Map<String, String> bankNameToCode = new java.util.HashMap<>();
        for (Map<String, Object> b : bankList) {
            bankNameToCode.put(str(b.get("bank_name")), str(b.get("bank_code")));
        }

        try (XSSFWorkbook wb = new XSSFWorkbook(file.getInputStream())) {
            Sheet sheet = wb.getSheetAt(0);

            for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                Row row = sheet.getRow(i);
                if (row == null) continue;

                try {
                    String memberNo = cellStr(row, 0);
                    String memberTypeNm = cellStr(row, 1);  // 개인/개인사업자/법인
                    String memberName = cellStr(row, 2);
                    String idNumber = cellStr(row, 3);
                    String phone = cellStr(row, 4);
                    String email = cellStr(row, 5);
                    String bankName = cellStr(row, 6);  // 은행명
                    String bankAccount = cellStr(row, 7).replaceAll("-", "");
                    String accountHolder = cellStr(row, 8);
                    String deductDay = cellStr(row, 9);
                    String deductAmount = cellStr(row, 10);
                    String startDate = cellStr(row, 11).replaceAll("-", "");
                    String endDate = cellStr(row, 12).replaceAll("-", "");
                    String cycleMonths = cellStr(row, 13); // 비어있으면 REGULAR
                    String memo = cellStr(row, 14);

                    if (!StringUtils.hasText(memberName)) continue;

                    // 구분 변환
                    String memberType = switch (memberTypeNm) {
                        case "개인사업자" -> "S";
                        case "법인" -> "C";
                        default -> "P";
                    };

                    // 은행코드 변환
                    String bankCode = bankNameToCode.get(bankName);
                    if (!StringUtils.hasText(bankCode)) {
                        log.error("[ExcelUpload] {}행 은행명 없음: {}", i + 1, bankName);
                        failed++;
                        continue;
                    }

                    // 약정일 변환 (말일 → 99)
                    if ("말일".equals(deductDay)) deductDay = "99";
                    else if (deductDay.length() == 1) deductDay = "0" + deductDay; // 1 → 01

                    // 결제주기 — 결제월 있으면 IRREGULAR, 없으면 REGULAR
                    String cycleType = StringUtils.hasText(cycleMonths) ? "IRREGULAR" : "REGULAR";
                    if ("REGULAR".equals(cycleType)) cycleMonths = null;

                    if (!StringUtils.hasText(endDate)) endDate = "99991231";

                    var param = new MapSqlParameterSource();
                    param.addValue("spjangcd", spjangcd);
                    param.addValue("memberType", memberType);
                    param.addValue("memberName", memberName);
                    param.addValue("idNumber", idNumber);
                    param.addValue("phone", phone);
                    param.addValue("email", email);
                    param.addValue("bankCode", bankCode);
                    param.addValue("bankAccount", bankAccount);
                    param.addValue("accountHolder", accountHolder);
                    param.addValue("deductDay", deductDay);
                    param.addValue("deductAmount", StringUtils.hasText(deductAmount) ? Long.parseLong(deductAmount.replaceAll(",", "")) : 0);
                    param.addValue("startDate", startDate);
                    param.addValue("endDate", endDate);
                    param.addValue("cycleType", cycleType);
                    param.addValue("cycleMonths", cycleMonths);
                    param.addValue("memo", memo);
                    param.addValue("userId", userId);

                    if (StringUtils.hasText(memberNo)) {
                        // UPDATE
                        param.addValue("memberNo", memberNo);
                        int cnt = sqlRunner.execute(/* skip_tenant_check */
                                """
                                        UPDATE cms_member SET
                                            member_type = :memberType, member_name = :memberName,
                                            id_number = :idNumber, phone = :phone, email = :email,
                                            bank_code = :bankCode, bank_account = :bankAccount,
                                            account_holder = :accountHolder, deduct_day = :deductDay,
                                            deduct_amount = :deductAmount, start_date = :startDate,
                                            end_date = :endDate, cycle_type = :cycleType,
                                            cycle_months = :cycleMonths,
                                            memo = :memo, _modifier_id = :userId, _modified = NOW()
                                        WHERE member_no = :memberNo AND spjangcd = :spjangcd
                                        """, param);
                        if (cnt > 0) updated++;
                        else failed++;
                    } else {
                        // INSERT
                        String newMemberNo = generateMemberNo(spjangcd, bankCode, idNumber);
                        param.addValue("memberNo", newMemberNo);
                        Map<String, Object> insertedRow = sqlRunner.getRow(/* skip_tenant_check */
                                """
                                        INSERT INTO cms_member (
                                            spjangcd, member_no, member_type, member_name,
                                            id_number, phone, email, bank_code, bank_account,
                                            account_holder, deduct_day, deduct_amount,
                                            start_date, end_date, cycle_type, cycle_months,
                                            status, memo, agree_yn,
                                            _creater_id, _created, _modifier_id, _modified
                                        ) VALUES (
                                            :spjangcd, :memberNo, :memberType, :memberName,
                                            :idNumber, :phone, :email, :bankCode, :bankAccount,
                                            :accountHolder, :deductDay, :deductAmount,
                                            :startDate, :endDate, :cycleType, :cycleMonths,
                                            'ACTIVE', :memo, 'N',
                                            :userId, NOW(), :userId, NOW()
                                        ) RETURNING id
                                        """, param);
                        Long savedId = ((Number) insertedRow.get("id")).longValue();
                        cmsAccountRegisterService.save(savedId, "1", null, null, userId);
                        inserted++;
                    }
                } catch (Exception e) {
                    log.error("[ExcelUpload] {}행 처리 실패: {}", i + 1, e.getMessage());
                    failed++;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("엑셀 파일 파싱 실패: " + e.getMessage());
        }

        return Map.of("inserted", inserted, "updated", updated, "failed", failed);
    }

    public void downloadTemplate(HttpServletResponse response) throws Exception {
        try (Workbook wb = new XSSFWorkbook()) {
            Sheet sheet = wb.createSheet("납부자");

            // 헤더 스타일
            CellStyle headerStyle = wb.createCellStyle();
            headerStyle.setFillForegroundColor(IndexedColors.LIGHT_CORNFLOWER_BLUE.getIndex());
            headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            Font headerFont = wb.createFont();
            headerFont.setBold(true);
            headerStyle.setFont(headerFont);

            // 필수항목 스타일 (빨간 글씨)
            CellStyle requiredStyle = wb.createCellStyle();
            requiredStyle.setFillForegroundColor(IndexedColors.LIGHT_CORNFLOWER_BLUE.getIndex());
            requiredStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            Font requiredFont = wb.createFont();
            requiredFont.setBold(true);
            requiredFont.setColor(IndexedColors.RED.getIndex());
            requiredStyle.setFont(requiredFont);

            // 예시 스타일
            CellStyle exampleStyle = wb.createCellStyle();
            exampleStyle.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
            exampleStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

            String[] headers = {
                    "납부자번호(수정시입력)", "구분(개인/개인사업자/법인)*", "납부자명*",
                    "생년월일/사업자번호", "연락처", "이메일",
                    "은행명*", "계좌번호*", "예금주명*",
                    "약정일(1~31, 말일)*", "출금금액*",
                    "청구시작일(YYYY-MM-DD)*", "청구종료일(YYYY-MM-DD, 빈칸=무제한)",
                    "결제월(비어있으면매월, 예:1,3,6,9)", "메모"
            };

            int[] requiredCols = {1, 2, 6, 7, 8, 9, 10, 11};
            java.util.Set<Integer> requiredSet = new java.util.HashSet<>();
            for (int c : requiredCols) requiredSet.add(c);

            Row headerRow = sheet.createRow(0);
            for (int i = 0; i < headers.length; i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(headers[i]);
                cell.setCellStyle(requiredSet.contains(i) ? requiredStyle : headerStyle);
                sheet.setColumnWidth(i, 6000);
            }

            // 예시 행 1 - 신규 (매월 정기)
            Object[] ex1 = {
                    "", "개인", "홍길동", "19900101", "01012345678", "hong@test.com",
                    "기업은행", "12345678901234", "홍길동",
                    "25", "100000", "2026-01-01", "", "", "예시-매월정기"
            };
            // 예시 행 2 - 신규 (비정기)
            Object[] ex2 = {
                    "", "법인", "테스트법인", "1234567890", "0212345678", "",
                    "국민은행", "98765432109876", "테스트법인",
                    "말일", "500000", "2026-01-01", "2027-12-31", "1,3,6,9", "예시-비정기"
            };
            // 예시 행 3 - 수정
            Object[] ex3 = {
                    "ZZ000001", "개인사업자", "김사업자", "1234567890", "01098765432", "",
                    "신한은행", "11223344556677", "김사업자",
                    "10", "200000", "2026-01-01", "", "", "예시-수정"
            };

            Object[][] examples = {ex1, ex2, ex3};
            for (int r = 0; r < examples.length; r++) {
                Row row = sheet.createRow(r + 1);
                for (int c = 0; c < examples[r].length; c++) {
                    Cell cell = row.createCell(c);
                    cell.setCellStyle(exampleStyle);
                    if (examples[r][c] != null) cell.setCellValue(examples[r][c].toString());
                }
            }

            response.setContentType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
            response.setHeader("Content-Disposition", "attachment; filename=member_template.xlsx");
            wb.write(response.getOutputStream());
        }
    }

    private String cellStr(Row row, int col) {
        Cell cell = row.getCell(col);
        if (cell == null) return "";
        return switch (cell.getCellType()) {
            case STRING -> cell.getStringCellValue().trim();
            case NUMERIC -> {
                if (DateUtil.isCellDateFormatted(cell)) {
                    yield new java.text.SimpleDateFormat("yyyyMMdd").format(cell.getDateCellValue());
                }
                yield String.valueOf((long) cell.getNumericCellValue());
            }
            case BOOLEAN -> String.valueOf(cell.getBooleanCellValue());
            default -> "";
        };
    }

    /**
     * cms_member 를 actcd 맵 / cltcd 맵으로 나눠 인덱싱한다.
     *
     * ★ actcd 와 cltcd 는 서로 다른 코드 체계이고 번호대가 겹친다.
     *   (예: 00745 → actcd로는 '154884이규프라자', cltcd로는 '옹진청과')
     *   따라서 절대 하나의 키로 합쳐서는 안 되고, actcd는 actcd끼리,
     *   cltcd는 cltcd끼리만 비교해야 한다.
     *
     * @return [0]=actcd맵, [1]=cltcd맵
     */
    private List<Map<String, Map<String, Object>>> indexMembers(List<Map<String, Object>> members) {
        Map<String, Map<String, Object>> byActcd = new java.util.HashMap<>();
        Map<String, Map<String, Object>> byCltcd = new java.util.HashMap<>();
        for (Map<String, Object> m : members) {
            String a = str(m.get("actcd"));
            String c = str(m.get("cltcd"));
            if (StringUtils.hasText(a)) byActcd.put(a, m);
            // ★ 현장 회원(actcd 보유)은 cltcd 맵에 넣지 않는다.
            //   현장 회원도 소속 거래처의 cltcd 를 함께 갖고 있어서, 그대로 담으면
            //   같은 거래처의 ②경로(거래처 단위) 동기화가 현장 회원을 자기 것으로 착각해
            //   계좌·납부자번호를 덮어쓴다. (CS메디컬프라자 ← 지원에셋플러스 사고)
            if (StringUtils.hasText(c) && !StringUtils.hasText(a)) byCltcd.put(c, m);
        }
        return List.of(byActcd, byCltcd);
    }

    /**
     * ERP 행에 대응하는 cms_member 를 찾는다.
     * ①경로(erp_actcd 있음) → actcd 로만 매칭
     * ②경로(erp_actcd 없음) → cltcd 로만 매칭
     */
    private Map<String, Object> matchMember(Map<String, Object> erpRow,
                                            Map<String, Map<String, Object>> byActcd,
                                            Map<String, Map<String, Object>> byCltcd) {
        if (erpRow == null) return null;
        String ea = str(erpRow.get("erp_actcd"));
        if (StringUtils.hasText(ea)) return byActcd.get(ea);
        String ec = str(erpRow.get("erp_cltcd"));
        return StringUtils.hasText(ec) ? byCltcd.get(ec) : null;
    }

    private String generateMemberNo(String spjangcd, String bankCode, String idNumber) {
        return generateMemberNo(spjangcd, bankCode, idNumber, false, null);
    }

    /**
     * 납부자번호 생성. ERP 프로시저(PROC_CMS_R mode=01)와 동일한 규칙을 따른다.
     *
     *   rnumchk = '1' → bnkcode(3) + LEFT(prenum, 6)                + 순번(2)  = 11자리
     *   그 외          → bnkcode(3) + SUBSTRING(saupnum, 6, 5)      + 순번(2)  = 10자리
     *
     * ★ 자릿수로 개인/사업자를 추정하면 안 된다. 웨스턴팰리스처럼 rnumchk=0 인데
     *   prenum 이 13자리(법인등록번호)인 거래처가 있어 오판한다.
     */
    private String generateMemberNo(String spjangcd, String bankCode, String idNumber,
                                    boolean useResident, String prenum) {
        // ★ 식별번호가 없으면 납부자번호를 만들 수 없다. 조용히 '00000'을 채우면
        //   {은행3}0000001 같은 번호가 은행에 등록되어 추적이 불가능해진다. (id=4192 사례)
        if (idNumber == null || idNumber.replaceAll("[^0-9]", "").isEmpty()) {
            throw new IllegalStateException(
                    "납부자번호 생성 불가 - 식별번호(사업자/주민)가 없습니다. bankCode=" + bankCode);
        }

        // 은행코드 3자리
        String bank = (bankCode != null && bankCode.length() >= 3) ? bankCode.substring(0, 3) : "000";

        String idPart;
        String prenumDigits = prenum != null ? prenum.replaceAll("[^0-9]", "") : "";
        if (useResident && prenumDigits.length() >= 6) {
            idPart = prenumDigits.substring(0, 6);        // 생년월일 6자리
        } else {
            String digits = idNumber.replaceAll("[^0-9]", "");
            if (digits.length() == 13) {
                idPart = digits.substring(0, 6);          // prenum 이 idNumber 로 넘어온 경우
            } else if (digits.length() >= 5) {
                idPart = digits.substring(digits.length() - 5);
            } else {
                idPart = String.format("%05d", Integer.parseInt(digits));
            }
        }

        // 순번 2자리 (중복 방지)
        String prefix = bank + idPart;
        // ★ 순번은 cms_member 뿐 아니라 cms_account_register(신청 이력)까지 함께 봐야 한다.
        //   계좌변경 시 cms_member.member_no 는 새 번호로 덮이므로, 회원 테이블만 보면
        //   이미 은행에 등록/해지된 옛 번호를 다시 뽑아 A016(이중신청)으로 거절된다.
        //
        // ★ 순번은 반드시 '뒤 2자리'로 읽는다. 납부자번호는 자리수가 섞여 있다.
        //     사업자(뒤5)  : 은행3 + 사업자5 + 순번2 = 10자리
        //     개인(생년월일): 은행3 + 생년월일6 + 순번2 = 11자리
        //   고정 위치(SUBSTRING(member_no,9,2))로 읽으면 11자리에서 한 칸 밀려
        //   '50','30' 같은 값을 순번으로 오인한다.
        //   정규식/길이 조건은 ERP 유래 코드(TW0189, N14614, 106662 등)가 섞였을 때
        //   CAST 예외로 죽는 것을 막는 방어다.
        Map<String, Object> seqRow = sqlRunner.getRow(/* skip_tenant_check */
                """
                SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq
                FROM (
                    SELECT CAST(RIGHT(member_no, 2) AS INTEGER) AS seq
                      FROM cms_member
                     WHERE spjangcd = :spjangcd AND member_no LIKE :prefix
                       AND member_no ~ '^[0-9]+$' AND LENGTH(member_no) >= 8
                    UNION ALL
                    SELECT CAST(RIGHT(member_no, 2) AS INTEGER)
                      FROM cms_account_register
                     WHERE spjangcd = :spjangcd AND member_no LIKE :prefix
                       AND member_no ~ '^[0-9]+$' AND LENGTH(member_no) >= 8
                ) t
                """,
                new MapSqlParameterSource("spjangcd", spjangcd)
                        .addValue("prefix", prefix + "%"));
        int seq = seqRow != null ? ((Number) seqRow.get("next_seq")).intValue() : 1;

        return prefix + String.format("%02d", seq);
    }


    /**
     * ERP에서 동기화 대상 행을 계산해 반환 — DB 쓰기 없음.
     * previewSync / applySync 공용 헬퍼.
     */
    private List<Map<String, Object>> fetchErpSyncRows(
            java.sql.Connection conn, String spjangcd, String custcd, int roundUnit,
            Set<String> excludeSet) throws Exception {

        // ★ 은행코드: TB_XBANK.bankcd = MS 내부코드(01,14,17...), bnkcode = 금결원 표준코드(088,004,012...).
        //   내부코드 체계는 ERP DB/법인마다 다르다(KYOUNG 17=카카오, TAEWON 17=농협중앙회).
        //   따라서 XCLIENT/E101 의 bankcd 로 TB_XBANK 를 조인해 bnkcode(표준)를 얻어야 한다.
        //   bankcd 는 유일키라 조인해도 행 복제 없음.
        Map<String, String> bnkCodeMap = new java.util.HashMap<>();
        bnkCodeMap.put("047","048");   // 신협 (TB_XBANK 047 → PG 048)
        bnkCodeMap.put("019","004");
        bnkCodeMap.put("006","004");   // 국민은행

        // ─────────────────────────────────────────────────────────────
        // ERP 프로시저 PROC_CMS_R(mode=01) 구조를 그대로 따른다.
        //
        //   src_path=1  A. 거래처 단위 (TB_XCLIENT.allchk=1)
        //                  키=cltcd, 이름=cltnm, 약정일=XCLIENT.autodate
        //                  납부자번호 = bnkcode + (rnumchk=1 ? prenum(1,6) : saupnum(6,5)) + '01'
        //   src_path=2  B. 현장 단위 (TB_E101.cmsflag=1, contg<>'04')
        //                  키=actcd, 이름=actnm, 약정일=E101.autodate
        //                  납부자번호 = bnkcode + E101.cmsnumber + '01'
        //   src_path=3  C. EB13 주도 (계약도 현장도 없는 거래처 구제)
        //                  키=cltcd, 이름=cltnm
        //
        // ★ actcd 와 cltcd 는 서로 다른 코드 체계이고 번호대가 겹친다(JUWON 73건 전부 상이).
        //   경로를 섞으면 남의 인증정보·납부자번호가 붙는다.
        // ★ 출금액은 매출(TB_DA023 미수)에서 잡으므로 deduct_amount 는 참고값이다.
        // ─────────────────────────────────────────────────────────────
        String today = "CONVERT(varchar(8),GETDATE(),112)";

        String sql =
                // ══════════ A. 거래처 단위 (allchk=1) ══════════
                "SELECT 1 AS src_path, C.cltcd AS cltcd, NULL AS actcd, C.cltnm AS member_name,"
                        + " C.corpperclafi AS corpperclafi, C.saupnum AS saupnum,"
                        + " C.rnumchk AS rnumchk, LTRIM(RTRIM(COALESCE(C.prenum,''))) AS prenum,"
                        + " CASE"
                        + "   WHEN NULLIF(REPLACE(LTRIM(RTRIM(C.cmsrnum)),'-',''),'') IS NOT NULL"
                        + "        THEN REPLACE(LTRIM(RTRIM(C.cmsrnum)),'-','')"
                        + "   WHEN NULLIF(REPLACE(LTRIM(RTRIM(C.saupnum)),'-',''),'') IS NOT NULL"
                        + "        THEN REPLACE(LTRIM(RTRIM(C.saupnum)),'-','')"
                        + "   WHEN C.prenum IS NOT NULL AND LEN(LTRIM(RTRIM(C.prenum)))=13"
                        + "        THEN LTRIM(RTRIM(C.prenum))"
                        + "   WHEN NULLIF(REPLACE(LTRIM(RTRIM(EB.SAUPNUM)),'-',''),'') IS NOT NULL"
                        + "        THEN REPLACE(LTRIM(RTRIM(EB.SAUPNUM)),'-','')"
                        + "   ELSE NULL END AS id_number,"
                        // resident_no = 주민번호 전용(13자리). 사업자번호는 biz_no 로 분리한다.
                        + " CASE WHEN C.prenum IS NOT NULL AND LEN(LTRIM(RTRIM(C.prenum)))=13"
                        + "      THEN LTRIM(RTRIM(C.prenum))"
                        + "      ELSE NULL END AS resident_no,"
                        + " NULLIF(REPLACE(LTRIM(RTRIM(COALESCE(C.saupnum,''))),'-',''),'') AS biz_no,"
                        + " LTRIM(RTRIM(COALESCE(B.bnkcode,''))) AS bank_code,"
                        + " REPLACE(REPLACE(LTRIM(RTRIM(COALESCE(C.accnum,''))),'-',''),' ','') AS bank_account,"
                        + " REPLACE(REPLACE(LTRIM(RTRIM(COALESCE(EB.CMSACCNUM,''))),'-',''),' ','') AS eb13_account,"
                        + " REPLACE(REPLACE(LTRIM(RTRIM(COALESCE(C.accnum,''))),'-',''),' ','') AS xclient_account,"
                        + " C.hptelnum AS phone, C.agneremail AS email,"
                        + " C.cltadres AS adresa, C.zipcd AS zipcd,"
                        + " NULL AS deduct_amount_raw,"
                        + " NULLIF(LTRIM(RTRIM(C.autodate)),'') AS deduct_day,"
                        + " NULLIF(LTRIM(RTRIM(C.autoflag)),'') AS auto_flag,"
                        // 계약기간은 ERP(E101)를 진실로 두고 CMS는 보관하지 않는다.
                        // 청구는 ERP 매출로 생성되므로 계약 종료 판정은 매출 유무가 대신한다.
                        // 여기서 실제 계약일을 넣으면 갱신 입력이 늦어질 때마다 CMS가 만료로
                        // 판정해 정상 청구를 막는다(한원빌딩 사례). 열어두고 고정한다.
                        + " '19000101' AS start_date, '99991231' AS end_date,"
                        + " EB.BANKCLTCD AS member_no,"
                        + " EB.SPFLAG AS eb13_spflag, EB.ENDFLAG AS eb13_endflag,"
                        + " NULL AS e101_cmsflag, NULL AS cmsnumber,"
                        + " K.delmon1,K.delmon2,K.delmon3,K.delmon4,K.delmon5,K.delmon6,"
                        + " K.delmon7,K.delmon8,K.delmon9,K.delmon10,K.delmon11,K.delmon12"
                        + " FROM TB_XCLIENT C WITH(NOLOCK)"
                        + " LEFT JOIN TB_XBANK B WITH(NOLOCK) ON C.bankcd=B.bankcd"
                        // 소속 현장 중 유효한 계약 1건(만료일 최장 → 금액 최대)에서 기간·주기를 취한다.
                        + " OUTER APPLY ("
                        + "     SELECT TOP 1 E1.stdate, E1.enddate,"
                        + "            E1.delmon1,E1.delmon2,E1.delmon3,E1.delmon4,E1.delmon5,E1.delmon6,"
                        + "            E1.delmon7,E1.delmon8,E1.delmon9,E1.delmon10,E1.delmon11,E1.delmon12"
                        + "       FROM TB_E601 E6 WITH(NOLOCK)"
                        + "       INNER JOIN TB_E101 E1 WITH(NOLOCK)"
                        + "           ON E1.actcd=E6.actcd AND E1.custcd=E6.custcd"
                        + "      WHERE E6.cltcd=C.cltcd AND E6.custcd=C.custcd"
                        + "        AND E1.enddate >= " + today
                        + "        AND E1.stdate  <= " + today
                        + "      ORDER BY E1.enddate DESC, E1.amt DESC) K"
                        // EB13 은 거래처 단위 등록분만 (ACTCD IS NULL)
                        + " LEFT JOIN TB_CMSEB13 EB WITH(NOLOCK)"
                        + "     ON EB.CUSTCD=C.custcd AND EB.ACTCD IS NULL AND EB.CLTCD=C.cltcd"
                        + "     AND PATINDEX('%[^0-9]%',LTRIM(RTRIM(EB.BANKCLTCD)))=0"
                        + "     AND LEN(LTRIM(RTRIM(EB.BANKCLTCD))) BETWEEN 10 AND 13"
                        + "     AND EB.SPDATE=(SELECT MAX(SPDATE) FROM TB_CMSEB13 WITH(NOLOCK)"
                        + "         WHERE CUSTCD=C.custcd AND ACTCD IS NULL AND CLTCD=C.cltcd"
                        + "         AND PATINDEX('%[^0-9]%',LTRIM(RTRIM(BANKCLTCD)))=0"
                        + "         AND LEN(LTRIM(RTRIM(BANKCLTCD))) BETWEEN 10 AND 13)"
                        + " WHERE C.custcd=? AND C.allchk=1"
                        + "   AND K.enddate IS NOT NULL"          // 유효 계약이 있는 거래처만
                        + "   AND NULLIF(LTRIM(RTRIM(C.accnum)),'') IS NOT NULL"
                        + "   AND ((C.cmsrnum IS NOT NULL AND LTRIM(RTRIM(C.cmsrnum))!='')"
                        + "     OR (C.saupnum IS NOT NULL AND LTRIM(RTRIM(C.saupnum))!='')"
                        + "     OR (C.prenum IS NOT NULL AND LEN(LTRIM(RTRIM(C.prenum)))=13))"

                        // ══════════ B. 현장 단위 (cmsflag=1) ══════════
                        + " UNION ALL"
                        + " SELECT 2 AS src_path, C.cltcd AS cltcd, E6.actcd AS actcd, E6.actnm AS member_name,"
                        + " C.corpperclafi AS corpperclafi, C.saupnum AS saupnum,"
                        + " C.rnumchk AS rnumchk, LTRIM(RTRIM(COALESCE(C.prenum,''))) AS prenum,"
                        + " CASE"
                        + "   WHEN NULLIF(LTRIM(RTRIM(E1.accnum)),'') IS NOT NULL THEN"
                        + "     COALESCE(NULLIF(REPLACE(LTRIM(RTRIM(E1.cmsrnum)),'-',''),''),"
                        + "              NULLIF(REPLACE(LTRIM(RTRIM(C.cmsrnum)),'-',''),''),"
                        + "              NULLIF(REPLACE(LTRIM(RTRIM(C.saupnum)),'-',''),''),"
                        + "              NULLIF(REPLACE(LTRIM(RTRIM(EB.SAUPNUM)),'-',''),''))"
                        + "   ELSE"
                        + "     COALESCE(NULLIF(REPLACE(LTRIM(RTRIM(C.cmsrnum)),'-',''),''),"
                        + "              NULLIF(REPLACE(LTRIM(RTRIM(C.saupnum)),'-',''),''),"
                        + "              NULLIF(REPLACE(LTRIM(RTRIM(EB.SAUPNUM)),'-',''),''))"
                        + "   END AS id_number,"
                        // resident_no = 주민번호 전용(13자리). 사업자번호는 biz_no 로 분리한다.
                        + " CASE WHEN C.prenum IS NOT NULL AND LEN(LTRIM(RTRIM(C.prenum)))=13"
                        + "      THEN LTRIM(RTRIM(C.prenum))"
                        + "      ELSE NULL END AS resident_no,"
                        + " NULLIF(REPLACE(LTRIM(RTRIM(COALESCE(C.saupnum,''))),'-',''),'') AS biz_no,"
                        // 계좌·은행은 같은 소스에서 짝으로 (E101 원장 우선, 없으면 XCLIENT)
                        + " CASE WHEN NULLIF(LTRIM(RTRIM(E1.accnum)),'') IS NOT NULL"
                        + "      THEN LTRIM(RTRIM(COALESCE(B1.bnkcode,'')))"
                        + "      ELSE LTRIM(RTRIM(COALESCE(B.bnkcode,''))) END AS bank_code,"
                        + " CASE WHEN NULLIF(LTRIM(RTRIM(E1.accnum)),'') IS NOT NULL"
                        + "      THEN REPLACE(REPLACE(LTRIM(RTRIM(E1.accnum)),'-',''),' ','')"
                        + "      ELSE REPLACE(REPLACE(LTRIM(RTRIM(COALESCE(C.accnum,''))),'-',''),' ','') END AS bank_account,"
                        + " REPLACE(REPLACE(LTRIM(RTRIM(COALESCE(EB.CMSACCNUM,''))),'-',''),' ','') AS eb13_account,"
                        + " REPLACE(REPLACE(LTRIM(RTRIM(COALESCE(C.accnum,''))),'-',''),' ','') AS xclient_account,"
                        + " C.hptelnum AS phone, C.agneremail AS email,"
                        + " C.cltadres AS adresa, C.zipcd AS zipcd,"
                        + " CASE"
                        + "     WHEN E1.contyul IS NOT NULL AND E1.contyul>0 AND E1.addyn=0"
                        + "          THEN E1.amt*(E1.contyul/100.0)*1.1"
                        + "     WHEN E1.contyul IS NOT NULL AND E1.contyul>0"
                        + "          THEN E1.amt*(E1.contyul/100.0)"
                        + "     WHEN E1.addyn=0 THEN E1.amt*1.1"
                        + "     ELSE E1.amt END AS deduct_amount_raw,"
                        + " COALESCE(NULLIF(LTRIM(RTRIM(E1.autodate)),''),NULLIF(LTRIM(RTRIM(C.autodate)),'')) AS deduct_day,"
                        + " COALESCE(NULLIF(LTRIM(RTRIM(E1.autoflag)),''),NULLIF(LTRIM(RTRIM(C.autoflag)),'')) AS auto_flag,"
                        // 계약기간 고정 — 사유는 A경로 주석 참조
                        + " '19000101' AS start_date, '99991231' AS end_date,"
                        + " EB.BANKCLTCD AS member_no,"
                        + " EB.SPFLAG AS eb13_spflag, EB.ENDFLAG AS eb13_endflag,"
                        + " E1.cmsflag AS e101_cmsflag,"
                        + " LTRIM(RTRIM(COALESCE(E1.cmsnumber,''))) AS cmsnumber,"
                        + " E1.delmon1,E1.delmon2,E1.delmon3,E1.delmon4,E1.delmon5,E1.delmon6,"
                        + " E1.delmon7,E1.delmon8,E1.delmon9,E1.delmon10,E1.delmon11,E1.delmon12"
                        + " FROM TB_E101 E1 WITH(NOLOCK)"
                        + " INNER JOIN TB_E601 E6 WITH(NOLOCK) ON E6.actcd=E1.actcd AND E6.custcd=E1.custcd"
                        + " INNER JOIN TB_XCLIENT C WITH(NOLOCK) ON C.cltcd=E6.cltcd AND C.custcd=E6.custcd"
                        + " LEFT JOIN TB_XBANK B  WITH(NOLOCK) ON C.bankcd=B.bankcd"
                        + " LEFT JOIN TB_XBANK B1 WITH(NOLOCK) ON E1.bankcd=B1.bankcd"
                        // EB13 은 현장 단위 등록분만 (ACTCD IS NOT NULL, CLTCD=actcd)
                        + " LEFT JOIN TB_CMSEB13 EB WITH(NOLOCK)"
                        + "     ON EB.CUSTCD=C.custcd AND EB.ACTCD IS NOT NULL AND EB.CLTCD=E6.actcd"
                        + "     AND PATINDEX('%[^0-9]%',LTRIM(RTRIM(EB.BANKCLTCD)))=0"
                        + "     AND LEN(LTRIM(RTRIM(EB.BANKCLTCD))) BETWEEN 10 AND 13"
                        + "     AND EB.SPDATE=(SELECT MAX(SPDATE) FROM TB_CMSEB13 WITH(NOLOCK)"
                        + "         WHERE CUSTCD=C.custcd AND ACTCD IS NOT NULL AND CLTCD=E6.actcd"
                        + "         AND PATINDEX('%[^0-9]%',LTRIM(RTRIM(BANKCLTCD)))=0"
                        + "         AND LEN(LTRIM(RTRIM(BANKCLTCD))) BETWEEN 10 AND 13)"
                        + " WHERE E1.custcd=? AND E1.cmsflag=1 AND ISNULL(E1.contg,'')<>'04'"
                        + "   AND E1.enddate >= " + today
                        + "   AND E1.stdate  <= " + today
                        + "   AND (NULLIF(LTRIM(RTRIM(E1.accnum)),'') IS NOT NULL"
                        + "     OR NULLIF(LTRIM(RTRIM(C.accnum)),'') IS NOT NULL)"
                        + "   AND ((E1.cmsrnum IS NOT NULL AND LTRIM(RTRIM(E1.cmsrnum))!='')"
                        + "     OR (C.cmsrnum IS NOT NULL AND LTRIM(RTRIM(C.cmsrnum))!='')"
                        + "     OR (C.saupnum IS NOT NULL AND LTRIM(RTRIM(C.saupnum))!=''))"
                        // 같은 actcd 에 계약이 여러 건이면 만료일이 가장 늦은 1건만
                        + "   AND E1.enddate=(SELECT MAX(enddate) FROM TB_E101 WITH(NOLOCK)"
                        + "       WHERE actcd=E1.actcd AND custcd=E1.custcd"
                        + "       AND enddate >= " + today + " AND stdate <= " + today + ")"

                        // ══════════ C. EB13 주도 (현장이 아예 없는 거래처) ══════════
                        + " UNION ALL"
                        + " SELECT 3 AS src_path, X.cltcd AS cltcd, NULL AS actcd, X.cltnm AS member_name,"
                        + " X.corpperclafi AS corpperclafi, X.saupnum AS saupnum,"
                        + " X.rnumchk AS rnumchk, LTRIM(RTRIM(COALESCE(X.prenum,''))) AS prenum,"
                        + " COALESCE(NULLIF(REPLACE(LTRIM(RTRIM(X.cmsrnum)),'-',''),''),"
                        + "          NULLIF(REPLACE(LTRIM(RTRIM(X.saupnum)),'-',''),''),"
                        + "          NULLIF(REPLACE(LTRIM(RTRIM(EB.SAUPNUM)),'-',''),'')) AS id_number,"
                        // resident_no = 주민번호 전용(13자리). 사업자번호는 biz_no 로 분리한다.
                        + " CASE WHEN X.prenum IS NOT NULL AND LEN(LTRIM(RTRIM(X.prenum)))=13"
                        + "      THEN LTRIM(RTRIM(X.prenum))"
                        + "      ELSE NULL END AS resident_no,"
                        + " NULLIF(REPLACE(LTRIM(RTRIM(COALESCE(X.saupnum,''))),'-',''),'') AS biz_no,"
                        + " LTRIM(RTRIM(COALESCE(XB2.bnkcode,''))) AS bank_code,"
                        + " REPLACE(REPLACE(LTRIM(RTRIM(COALESCE(X.accnum,''))),'-',''),' ','') AS bank_account,"
                        + " REPLACE(REPLACE(LTRIM(RTRIM(COALESCE(EB.CMSACCNUM,''))),'-',''),' ','') AS eb13_account,"
                        + " REPLACE(REPLACE(LTRIM(RTRIM(COALESCE(X.accnum,''))),'-',''),' ','') AS xclient_account,"
                        + " X.hptelnum AS phone, X.agneremail AS email,"
                        + " X.cltadres AS adresa, X.zipcd AS zipcd,"
                        + " NULL AS deduct_amount_raw,"
                        + " NULLIF(LTRIM(RTRIM(X.autodate)),'') AS deduct_day,"
                        + " NULLIF(LTRIM(RTRIM(X.autoflag)),'') AS auto_flag,"
                        // C경로는 현장(E601)이 없어 E101 계약 자체가 존재하지 않는다.
                        // 기존에는 NULL 이라 start_date 조건이 걸린 화면에서 회원이 통째로
                        // 누락됐다(SQL 에서 NULL 비교는 UNKNOWN). A/B 와 같은 값으로 맞춘다.
                        + " '19000101' AS start_date, '99991231' AS end_date,"
                        + " EB.BANKCLTCD AS member_no,"
                        + " EB.SPFLAG AS eb13_spflag, EB.ENDFLAG AS eb13_endflag,"
                        + " NULL AS e101_cmsflag, NULL AS cmsnumber,"
                        + " NULL AS delmon1,NULL AS delmon2,NULL AS delmon3,NULL AS delmon4,"
                        + " NULL AS delmon5,NULL AS delmon6,NULL AS delmon7,NULL AS delmon8,"
                        + " NULL AS delmon9,NULL AS delmon10,NULL AS delmon11,NULL AS delmon12"
                        + " FROM TB_CMSEB13 EB WITH(NOLOCK)"
                        + " INNER JOIN TB_XCLIENT X WITH(NOLOCK) ON X.custcd=EB.CUSTCD AND X.cltcd=EB.CLTCD"
                        + " LEFT JOIN TB_XBANK XB2 WITH(NOLOCK) ON X.bankcd=XB2.bankcd"
                        + " WHERE EB.CUSTCD=? AND EB.SPFLAG='1' AND EB.ENDFLAG='Y'"
                        + "   AND EB.ACTCD IS NULL"
                        + "   AND NULLIF(LTRIM(RTRIM(X.accnum)),'') IS NOT NULL"
                        + "   AND PATINDEX('%[^0-9]%',LTRIM(RTRIM(EB.BANKCLTCD)))=0"
                        + "   AND LEN(LTRIM(RTRIM(EB.BANKCLTCD))) BETWEEN 10 AND 13"
                        + "   AND EB.SPDATE=(SELECT MAX(SPDATE) FROM TB_CMSEB13 WITH(NOLOCK)"
                        + "       WHERE CLTCD=EB.CLTCD AND CUSTCD=EB.CUSTCD AND ENDFLAG='Y'"
                        + "       AND PATINDEX('%[^0-9]%',LTRIM(RTRIM(BANKCLTCD)))=0"
                        + "       AND LEN(LTRIM(RTRIM(BANKCLTCD))) BETWEEN 10 AND 13)"
                        // A/B 경로에서 이미 잡히는 거래처는 제외
                        + "   AND NOT EXISTS (SELECT 1 FROM TB_E601 E6b WITH(NOLOCK)"
                        + "       WHERE E6b.custcd=EB.CUSTCD AND E6b.cltcd=EB.CLTCD)"
                        + "   AND NOT EXISTS (SELECT 1 FROM TB_E601 E6c WITH(NOLOCK)"
                        + "       WHERE E6c.custcd=EB.CUSTCD AND E6c.actcd=EB.CLTCD)";

        // ★ 한 거래처(cltcd)에 CMS 대상 현장(actcd)이 여럿이면 EB13 인증이 공유되어
        //   같은 BANKCLTCD가 여러 회원에게 붙는다. (예: 지원에셋플러스 00811 → 00756/00779,
        //   윤득선 00839 → 00764/00765) 납부자번호가 중복되면 은행이 A016(이중신청)으로 거절하므로,
        //   먼저 잡힌 한 건만 EB13 번호를 쓰고 나머지는 규칙으로 새 번호를 만든다.
        Set<String> usedPayerNo = new java.util.HashSet<>();

        List<Map<String, Object>> result = new java.util.ArrayList<>();
        try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, custcd);   // A. 거래처 단위
            ps.setString(2, custcd);   // B. 현장 단위
            ps.setString(3, custcd);   // C. EB13 주도
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    // ★ 회원 식별 키
                    //   src_path=2(현장 단위)만 actcd 를 쓰고, 1(거래처)/3(EB13주도)은 cltcd 를 쓴다.
                    //   두 코드 체계는 번호대가 겹치므로 경로를 반드시 구분해야 한다.
                    int    srcPath  = rs.getInt("src_path");
                    String rowActcd = (srcPath == 2) ? rs.getString("actcd") : null;
                    String rowCltcd = rs.getString("cltcd");
                    String actcd    = StringUtils.hasText(rowActcd) ? rowActcd : rowCltcd;  // = syncKey
                    if (excludeSet.contains(actcd)) continue;

                    String autoFlag  = rs.getString("auto_flag");
                    String deductDay = rs.getString("deduct_day");
                    if ("1".equals(autoFlag)) {
                        deductDay = "99";
                    } else if (deductDay == null || deductDay.trim().isEmpty()) {
                        log.warn("[ERP동기화] deduct_day 없음 - 스킵: actcd={}", actcd);
                        continue;
                    } else {
                        try { deductDay = String.format("%02d", Integer.parseInt(deductDay.trim())); }
                        catch (NumberFormatException e) {
                            log.warn("[ERP동기화] deduct_day 파싱 실패 - 스킵: actcd={}", actcd);
                            continue;
                        }
                    }

                    String startDate = cleanDate(rs.getString("start_date"));
                    String endDate   = cleanDate(rs.getString("end_date"));
                    // 쿼리에서 리터럴로 고정하므로 아래 보정은 실제로는 타지 않는다.
                    // 수동 등록·엑셀 업로드 등 다른 경로와 값을 맞추기 위한 안전장치로 남긴다.
                    if (startDate == null || startDate.isEmpty()) startDate = "19000101";
                    if (endDate == null || endDate.isEmpty()) endDate = "99991231";

                    String bnkCode     = rs.getString("bank_code");
                    String bankCode    = bnkCodeMap.getOrDefault(bnkCode, bnkCode);
                    String bankAccount = rs.getString("bank_account");
                    double rawAmt2     = rs.getDouble("deduct_amount_raw");
                    Long   deductAmt   = rs.wasNull() ? null : roundAmount(rawAmt2, roundUnit);

                    List<String> months = new java.util.ArrayList<>();
                    for (int i = 1; i <= 12; i++) {
                        String mon = rs.getString("delmon" + i);
                        if (mon != null && !mon.trim().isEmpty())
                            months.add(String.valueOf(Integer.parseInt(mon.trim())));
                    }
                    String cycleMonths = months.isEmpty() ? null : String.join(",", months);
                    String cycleType   = (months.size() == 12) ? "REGULAR" : "IRREGULAR";
                    if ("REGULAR".equals(cycleType)) cycleMonths = null;

                    String saupnum      = rs.getString("saupnum");
                    String corpperclafi = rs.getString("corpperclafi");
                    String idNumber     = rs.getString("id_number");
                    String erpMemberNo  = rs.getString("member_no");
                    if ("null".equalsIgnoreCase(erpMemberNo)) erpMemberNo = null;

                    // EB13 인증완료 판정: SPFLAG='1'(신규) AND ENDFLAG='Y'(완료)일 때만 Y.
                    // (해지건 SPFLAG='3'/ENDFLAG='Y' 를 인증완료로 오인하지 않도록)
                    String eb13Spflag  = rs.getString("eb13_spflag");
                    String eb13Endflag = rs.getString("eb13_endflag");
                    String agreeYn = ("1".equals(eb13Spflag) && "Y".equals(eb13Endflag)) ? "Y" : "N";

                    // ★ 현장 단위 CMS(cmsflag=1)는 납부자번호 규칙이 다르다.
                    //   bnkcode(3) + E101.cmsnumber(5) + '01'  (PROC_CMS_R mode=01 B분기)
                    //   예) CS메디컬프라자: 020 + 24001 + 01 = 0202400101
                    if (!StringUtils.hasText(erpMemberNo)) {
                        String cmsNumber = rs.getString("cmsnumber");
                        if (srcPath == 2 && StringUtils.hasText(cmsNumber)) {
                            String bank3 = (bankCode != null && bankCode.length() >= 3)
                                    ? bankCode.substring(0, 3) : "000";
                            erpMemberNo = bank3 + cmsNumber.trim() + "01";
                        }
                    }

                    // 같은 EB13 번호를 이미 다른 현장이 가져갔으면 공유 불가 → 새로 생성
                    if (StringUtils.hasText(erpMemberNo) && !usedPayerNo.add(erpMemberNo)) {
                        log.warn("[ERP동기화] 납부자번호 중복(거래처 공유 EB13) key={} name={} payer={} → 신규 생성",
                                actcd, rs.getString("member_name"), erpMemberNo);
                        erpMemberNo = null;
                    }

                    // EB13(BANKCLTCD) 없는 신규 → 규칙으로 납부자번호 생성.
                    // 식별번호가 없어 생성이 불가하면 null로 두고 행 자체는 살린다.
                    // (미리보기에 노출은 하되, 반영 시 담당자가 식별번호를 채우도록)
                    if (!StringUtils.hasText(erpMemberNo)) {
                        try {
                            String gen = generateMemberNo(spjangcd, bankCode, idNumber,
                                    "1".equals(rs.getString("rnumchk")), rs.getString("prenum"));
                            // 같은 실행 안에서 또 겹치면 순번을 올려 회피
                            int guard = 0;
                            while (!usedPayerNo.add(gen) && guard++ < 90) {
                                int seq = Integer.parseInt(gen.substring(gen.length() - 2)) + 1;
                                gen = gen.substring(0, gen.length() - 2) + String.format("%02d", seq);
                            }
                            erpMemberNo = gen;
                        } catch (Exception e) {
                            log.warn("[ERP동기화] 납부자번호 생성 실패 key={} name={}: {}",
                                    actcd, rs.getString("member_name"), e.getMessage());
                            erpMemberNo = null;
                        }
                    }

                    String memberType;
                    if (StringUtils.hasText(saupnum)) {
                        memberType = "0".equals(corpperclafi) ? "S" : "C";
                    } else if ("0".equals(corpperclafi) && StringUtils.hasText(idNumber)
                            && idNumber.replaceAll("[^0-9]","").length() == 13) {
                        memberType = "P";
                    } else {
                        memberType = "S";
                    }

                    Map<String, Object> row = new java.util.LinkedHashMap<>();
                    row.put("sync_key",      actcd);     // 화면/선택 식별용 표시 키 (매칭은 erp_actcd/erp_cltcd로)
                    row.put("cltcd",         actcd);     // 하위호환(화면 표시용). 실제 cltcd는 erp_cltcd 참조
                    row.put("erp_actcd",     rowActcd);  // ②경로는 null
                    row.put("erp_cltcd",     rowCltcd);  // 항상 실제 XCLIENT.cltcd
                    row.put("member_name",   rs.getString("member_name"));
                    row.put("member_type",   memberType);
                    row.put("id_number",     idNumber);
                    row.put("resident_no",   rs.getString("resident_no"));
                    row.put("biz_no",        rs.getString("biz_no"));
                    row.put("bank_code",     bankCode);
                    row.put("bank_account",  bankAccount);
                    row.put("xclient_account", rs.getString("xclient_account"));
                    row.put("erp_member_no", erpMemberNo);
                    row.put("agree_yn",      agreeYn);
                    row.put("eb13_spflag",   eb13Spflag);
                    row.put("eb13_endflag",  eb13Endflag);
                    row.put("phone",         rs.getString("phone"));
                    row.put("email",         rs.getString("email"));
                    row.put("adresa",        rs.getString("adresa"));
                    row.put("zipcd",         rs.getString("zipcd"));
                    row.put("deduct_amount", deductAmt);
                    row.put("deduct_day",    deductDay);
                    row.put("start_date",    startDate);
                    row.put("end_date",      endDate);
                    row.put("cycle_type",    cycleType);
                    row.put("cycle_months",  cycleMonths);
                    row.put("deduct_month_type", "2".equals(autoFlag) ? "NEXT" : "CURRENT");
                    result.add(row);
                }
            }
        }
        return result;
    }

    /** ERP 동기화 미리보기 — 읽기 전용, UPDATE 없음 */
    public Map<String, Object> previewSync(String spjangcd) {
        log.info("[ERP미리보기] 시작 spjangcd={}", spjangcd);

        Map<String, Object> erp = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT host, port, db_name, username, password, custcd, ms_spjangcd FROM tb_xa012_erp WHERE spjangcd = :spjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd));
        if (erp == null) throw new IllegalStateException("ERP 접속정보가 없습니다.");
        String custcd     = str(erp.get("custcd"));
        String msSpjangcd = str(erp.get("ms_spjangcd"));

        Map<String, Object> cms = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT is_normal_status, amount_round_unit FROM tb_xa012_cms WHERE spjangcd = :spjangcd AND ms_spjangcd IS NOT DISTINCT FROM :msSpjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd)
                        .addValue("msSpjangcd", msSpjangcd.isEmpty() ? null : msSpjangcd));
        if (cms == null || !Boolean.TRUE.equals(cms.get("is_normal_status")))
            throw new IllegalStateException("CMS 서비스 상태가 승인이 아닙니다.");
        int roundUnit = cms.get("amount_round_unit") != null ? ((Number) cms.get("amount_round_unit")).intValue() : 1;

        Set<String> excludeSet = sqlRunner.getRows(/* skip_tenant_check */
                        "SELECT cltcd FROM cms_member_sync_exclude WHERE spjangcd = :spjangcd",
                        new MapSqlParameterSource("spjangcd", spjangcd))
                .stream().map(r -> str(r.get("cltcd"))).collect(java.util.stream.Collectors.toSet());

        // cms_member 현재값
        List<Map<String, Object>> members = sqlRunner.getRows(/* skip_tenant_check */
                "SELECT id, cltcd, actcd, member_no, member_name, member_type, id_number, resident_no, biz_no, bank_code, bank_account, deduct_amount, deduct_day, agree_yn, sync_confirmed_ref FROM cms_member WHERE spjangcd = :spjangcd AND status <> 'INACTIVE'",
                new MapSqlParameterSource("spjangcd", spjangcd));
        // ★ 해지 회원은 제외한다. INACTIVE 행이 키를 점유하면 같은 코드의 신규가 영원히 가려진다.
        //   (2026-07-23 중복적재 건이 actcd 00882를 점유해 웨스턴팰리스호텔이 안 보이던 사고)
        List<Map<String, Map<String, Object>>> idx = indexMembers(members);
        Map<String, Map<String, Object>> byActcd = idx.get(0);
        Map<String, Map<String, Object>> byCltcd = idx.get(1);

        // 최근 billing result_code + 실패 계좌 (실패계좌 배제 추천용)
        List<Map<String, Object>> billingRows = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT DISTINCT ON (b.member_id) b.member_id, b.result_code,
                       b.bank_account AS failed_account, b.result_msg
                FROM cms_billing b
                WHERE b.spjangcd = :spjangcd
                ORDER BY b.member_id, b.deduct_date DESC NULLS LAST, b._created DESC NULLS LAST
                """,
                new MapSqlParameterSource("spjangcd", spjangcd));
        Map<Long, String> billingMap = new java.util.HashMap<>();
        Map<Long, String> billingMsgMap = new java.util.HashMap<>();
        // member_id → 최근 실패 청구에 쓰인 계좌(정규화). 성공(0000)/잔액부족(0021)은 계좌문제 아님 → 배제대상서 제외
        Map<Long, String> failedAcctMap = new java.util.HashMap<>();
        java.util.Set<String> acctFailCodes = java.util.Set.of("0017", "0019", "0012", "0013", "0014");
        for (Map<String, Object> br : billingRows) {
            Object mid = br.get("member_id");
            if (mid == null) continue;
            long id = ((Number) mid).longValue();
            String rc = str(br.get("result_code"));
            billingMap.put(id, rc);
            billingMsgMap.put(id, str(br.get("result_msg")));
            if (acctFailCodes.contains(rc)) {
                String fa = str(br.get("failed_account")).replaceAll("[^0-9]", "");
                if (!fa.isEmpty()) failedAcctMap.put(id, fa);
            }
        }
        java.util.Set<String> successCodes = java.util.Set.of("0000", "0021");

        String url = String.format("jdbc:sqlserver://%s:%s;databaseName=%s;encrypt=false",
                str(erp.get("host")), str(erp.get("port")), str(erp.get("db_name")));
        try { Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); }
        catch (ClassNotFoundException e) { throw new IllegalStateException("MSSQL 드라이버 없음"); }

        List<Map<String, Object>> newRows        = new java.util.ArrayList<>();
        List<Map<String, Object>> changedRows    = new java.util.ArrayList<>();
        List<Map<String, Object>> unverifiedRows = new java.util.ArrayList<>();
        int sameCount = 0; // 청구 성공 중 → 보호

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                url, str(erp.get("username")), str(erp.get("password")))) {

            // ERP 행을 회원별로 짝지어 둔다.
            //  ★ ①경로는 actcd끼리, ②경로는 cltcd끼리만 비교한다. (indexMembers 주석 참고)
            Map<Long, Map<String, Object>> erpByMemberId = new java.util.HashMap<>();
            for (Map<String, Object> erpRow : fetchErpSyncRows(conn, spjangcd, custcd, roundUnit, excludeSet)) {
                Map<String, Object> matched = matchMember(erpRow, byActcd, byCltcd);
                if (matched == null) {
                    // [1] 신규: ERP 활성이지만 cms_member에 없는 계정
                    newRows.add(erpRow);
                    continue;
                }
                Long mid = ((Number) matched.get("id")).longValue();
                erpByMemberId.put(mid, erpRow);
            }

            // [2] 기존 cms_member 분류 — 최근 청구 결과가 진실의 기준
            for (Map<String, Object> existing : members) {
                String cltcd = str(existing.get("actcd"));
                if (!StringUtils.hasText(cltcd)) cltcd = str(existing.get("cltcd"));

                Object midObj         = existing.get("id");
                Long   memberId       = midObj != null ? ((Number) midObj).longValue() : null;
                String lastResultCode = memberId != null ? billingMap.get(memberId) : null;

                // [2-1] 청구 성공 중 → 현재값이 실제 작동값, 보호
                if (lastResultCode != null && successCodes.contains(lastResultCode)) {
                    sameCount++;
                    continue;
                }

                // [2-0] 담당자가 이미 확인/확정했고, 그 이후 ERP값이 안 바뀌었으면 제외
                //       (ERP값이 또 바뀌면 ref가 달라져 다시 노출됨)
                Map<String, Object> erpRowChk = memberId != null ? erpByMemberId.get(memberId) : null;
                String confirmedRef = str(existing.get("sync_confirmed_ref"));
                if (erpRowChk != null && !confirmedRef.isEmpty()
                        && confirmedRef.equals(syncRef(erpRowChk))) {
                    sameCount++;
                    continue;
                }

                // [2-2] 청구 이력 없음 → 수동 검증 대상
                if (lastResultCode == null) {
                    Map<String, Object> uRow = new java.util.LinkedHashMap<>();
                    // ★ ERP 상호를 함께 실어야 이름 불일치를 화면에서 즉시 발견할 수 있다.
                    //   (cms_member "인스타" ↔ ERP "N27039웨스턴팰리스호텔" 같은 키 충돌 사례)
                    Map<String, Object> uErp = memberId != null ? erpByMemberId.get(memberId) : null;
                    uRow.put("cltcd",           cltcd);
                    uRow.put("member_name",     existing.get("member_name"));
                    uRow.put("erp_member_name", uErp != null ? uErp.get("member_name") : null);
                    uRow.put("in_erp",          uErp != null);
                    uRow.put("has_eb13",        uErp != null && "Y".equals(str(uErp.get("agree_yn"))));
                    unverifiedRows.add(uRow);
                    continue;
                }

                // [2-3] 값문제 실패(0017/0012/0013/0014 등) → 교정 대상
                Map<String, Object> erpRow = memberId != null ? erpByMemberId.get(memberId) : null;

                // ERP CMS 대상에 없는 회원(웹 인증분 등)은 비교/반영 대상 아님 → 패스
                if (erpRow == null) {
                    sameCount++;
                    continue;
                }

                String curMemberNo = str(existing.get("member_no"));
                String newMemberNo = !StringUtils.hasText(curMemberNo)
                        ? str(erpRow.get("erp_member_no")) : curMemberNo;

                String curAmt = existing.get("deduct_amount") != null
                        ? Long.toString(((Number) existing.get("deduct_amount")).longValue()) : "";
                String newAmt = erpRow.get("deduct_amount") != null
                        ? erpRow.get("deduct_amount").toString() : "";

                // ── 추천 계좌 결정: "실패한 계좌를 배제 + XCLIENT(원장) 우선" ──
                //   세 소스: eb13Acc(erpRow.bank_account), xclientAcc(erpRow.xclient_account), curAcc(cms_member)
                //   billing에서 계좌문제(0017/0019 등)로 실패한 계좌는 추천에서 제외한다.
                String eb13Acc    = str(erpRow.get("bank_account")).replaceAll("[^0-9]", "");
                String xclientAcc = str(erpRow.get("xclient_account")).replaceAll("[^0-9]", "");
                String curAcc     = str(existing.get("bank_account")).replaceAll("[^0-9]", "");
                String failedAcc  = memberId != null ? failedAcctMap.get(memberId) : null;

                // 추천 우선순위: 거래처 회원은 XCLIENT(원장) → EB13.
                //  ★ 현장 단위 회원(erp_actcd 보유)은 반대로 EB13 → XCLIENT.
                //    현장의 xclient_account 는 '소속 거래처'의 계좌라, 그대로 쓰면 같은 거래처의
                //    다른 회원과 계좌가 같아져 엉뚱한 곳에서 출금된다.
                boolean isSite = StringUtils.hasText(str(erpRow.get("erp_actcd")));
                String[] accOrder = isSite ? new String[]{ eb13Acc, xclientAcc }
                        : new String[]{ xclientAcc, eb13Acc };

                String recommendedAcc = null;
                for (String cand : accOrder) {
                    if (cand == null || cand.isEmpty()) continue;
                    if (failedAcc != null && cand.equals(failedAcc)) continue; // 실패계좌 배제
                    recommendedAcc = cand;
                    break;
                }
                // 후보가 다 실패계좌뿐이면(=배제할 게 없으면) 같은 우선순위로라도 채움
                if (recommendedAcc == null) {
                    for (String cand : accOrder) {
                        if (cand != null && !cand.isEmpty()) { recommendedAcc = cand; break; }
                    }
                    if (recommendedAcc == null) recommendedAcc = curAcc;
                }

                List<Map<String, Object>> keyChanges  = new java.util.ArrayList<>();
                List<Map<String, Object>> infoChanges = new java.util.ArrayList<>();
                syncCheckField(keyChanges, "bank_code",    str(existing.get("bank_code")),    str(erpRow.get("bank_code")));
                // 계좌: 추천계좌(실패배제+원장우선)로 비교. 은행코드는 통합/자체 달라 비교 제외.
                syncCheckField(keyChanges, "bank_account", curAcc,                            recommendedAcc);
                syncCheckField(keyChanges, "member_no",    curMemberNo,                       newMemberNo);
                syncCheckField(keyChanges, "id_number",    str(existing.get("id_number")),    str(erpRow.get("id_number")));
                syncCheckField(infoChanges, "member_name",   str(existing.get("member_name")), str(erpRow.get("member_name")));
                syncCheckField(infoChanges, "deduct_amount", curAmt,                           newAmt);
                syncCheckField(infoChanges, "deduct_day",    str(existing.get("deduct_day")),  str(erpRow.get("deduct_day")));

                // 실제로 바뀐 필드가 하나도 없으면 제외
                if (keyChanges.isEmpty() && infoChanges.isEmpty()) {
                    sameCount++;
                    continue;
                }

                Map<String, Object> item = new java.util.LinkedHashMap<>();
                item.put("cltcd",            cltcd);
                item.put("member_name",      existing.get("member_name"));
                item.put("last_result_code", lastResultCode);
                item.put("no_eb13",          false);
                item.put("key_changes",      keyChanges);
                item.put("info_changes",     infoChanges);
                item.put("new_values", erpRow);
                // 인증 상태(화면 표시용): 웹 현재값 / ERP(EB13) 기준값
                item.put("agree_yn",     existing.get("agree_yn"));
                item.put("erp_agree_yn", erpRow.get("agree_yn"));
                item.put("eb13_spflag",  erpRow.get("eb13_spflag"));
                item.put("eb13_endflag", erpRow.get("eb13_endflag"));

                // ── 3층 신호 (화면 정렬·강조용) ──
                boolean sigBilling  = lastResultCode != null && acctFailCodes.contains(lastResultCode);
                boolean sigEb13Diff = !curAcc.equals(eb13Acc) && !eb13Acc.isEmpty();
                boolean sigLedgerDiff = !eb13Acc.isEmpty() && !xclientAcc.isEmpty()
                        && !eb13Acc.equals(xclientAcc);   // EB13 vs 원장(XCLIENT)
                item.put("sig_billing_fail", sigBilling);
                item.put("sig_eb13_diff",    sigEb13Diff);
                item.put("sig_ledger_diff",  sigLedgerDiff);
                item.put("last_result_msg",  memberId != null ? billingMsgMap.get(memberId) : null);
                // 신호 개수 → 정렬 가중치(많을수록 위)
                int sigCount = (sigBilling?1:0) + (sigEb13Diff?1:0) + (sigLedgerDiff?1:0);
                item.put("sig_count", sigCount);
                // 계좌 세 소스 원본(화면 참고 표시용)
                item.put("acc_member",  curAcc);
                item.put("acc_eb13",    eb13Acc);
                item.put("acc_xclient", xclientAcc);
                item.put("acc_recommended", recommendedAcc);
                item.put("failed_account", failedAcc);

                changedRows.add(item);
            }
            // 신호 많은 순으로 정렬 (billing실패+양쪽불일치가 최상단)
            changedRows.sort((a, b) -> Integer.compare(
                    (int) b.getOrDefault("sig_count", 0),
                    (int) a.getOrDefault("sig_count", 0)));
        } catch (Exception e) {
            throw new IllegalStateException("MSSQL 접속 실패: " + e.getMessage());
        }

        log.info("[ERP미리보기] 완료 spjangcd={} 신규={} 교정대상={} 성공중보호={} 이력없음={}",
                spjangcd, newRows.size(), changedRows.size(), sameCount, unverifiedRows.size());

        // 교정 대상 필드별 집계
        java.util.Set<String> verifyTargets = java.util.Set.of("114631", "161305", "130437");
        List<String> bankAccCltcds    = new java.util.ArrayList<>();
        List<String> bankCodeCltcds   = new java.util.ArrayList<>();
        List<String> idNumCltcds      = new java.util.ArrayList<>();
        List<String> idNumOnlyCltcds  = new java.util.ArrayList<>();
        List<String> payerCltcds      = new java.util.ArrayList<>();
        Map<String, String> verifyInChanged = new java.util.LinkedHashMap<>();

        for (Map<String, Object> item : changedRows) {
            String itemCltcd = str(item.get("cltcd"));
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> kc = (List<Map<String, Object>>) item.get("key_changes");
            if (kc == null) kc = java.util.Collections.emptyList();
            java.util.Set<String> cf = kc.stream()
                    .map(c -> str(c.get("field")))
                    .collect(java.util.stream.Collectors.toSet());

            if (cf.contains("bank_account")) bankAccCltcds.add(itemCltcd);
            if (cf.contains("bank_code"))    bankCodeCltcds.add(itemCltcd);
            if (cf.contains("id_number"))    idNumCltcds.add(itemCltcd);
            if (cf.contains("member_no"))    payerCltcds.add(itemCltcd);
            if (cf.contains("id_number") && !cf.contains("bank_account")
                    && !cf.contains("bank_code") && !cf.contains("member_no"))
                idNumOnlyCltcds.add(itemCltcd);
            if (verifyTargets.contains(itemCltcd))
                verifyInChanged.put(itemCltcd, str(item.get("last_result_code")));
        }
        // 검증 3건 미포함 표시
        for (String vc : verifyTargets) {
            if (!verifyInChanged.containsKey(vc)) verifyInChanged.put(vc, "미포함(보호또는이력없음)");
        }

        // ── 교정대상 필드별 로그 ──────────────────────────────────────────
        log.info("[ERP미리보기][집계] ========= 교정대상 {}건 필드별 분석 =========",
                changedRows.size());
        log.info("[ERP미리보기][집계] ★위험★ 계좌(bank_account) 변경: {}건 → {}",
                bankAccCltcds.size(), bankAccCltcds);
        log.info("[ERP미리보기][집계] ★위험★ 은행코드(bank_code) 변경: {}건 → {}",
                bankCodeCltcds.size(), bankCodeCltcds);
        log.info("[ERP미리보기][집계]         payer(member_no) 변경: {}건 → {}",
                payerCltcds.size(), payerCltcds);
        log.info("[ERP미리보기][집계]         식별번호(id_number) 변경: {}건 (이 중 id_number만 바뀌는 건: {}건 → {})",
                idNumCltcds.size(), idNumOnlyCltcds.size(), idNumOnlyCltcds);
        log.info("[ERP미리보기][집계] 검증 3건 포함여부: {}", verifyInChanged);
        log.info("[ERP미리보기][집계] ==============================================");
        // ────────────────────────────────────────────────────────────────

        Map<String, Object> fieldStats = new java.util.LinkedHashMap<>();
        fieldStats.put("bank_account_changed_count",  bankAccCltcds.size());
        fieldStats.put("bank_account_changed_cltcds", bankAccCltcds);
        fieldStats.put("bank_code_changed_count",     bankCodeCltcds.size());
        fieldStats.put("bank_code_changed_cltcds",    bankCodeCltcds);
        fieldStats.put("id_number_changed_count",     idNumCltcds.size());
        fieldStats.put("id_number_only_count",        idNumOnlyCltcds.size());
        fieldStats.put("id_number_only_cltcds",       idNumOnlyCltcds);
        fieldStats.put("member_no_changed_count",     payerCltcds.size());
        fieldStats.put("member_no_changed_cltcds",    payerCltcds);
        fieldStats.put("verify_records",              verifyInChanged);

        Map<String, Object> result = new java.util.LinkedHashMap<>();
        result.put("spjangcd",         spjangcd);
        result.put("new_count",        newRows.size());
        result.put("same_count",       sameCount);             // 청구 성공 중, 보호
        result.put("changed_count",    changedRows.size());    // 값문제 실패 → 교정
        result.put("unverified_count", unverifiedRows.size()); // 이력 없음 → 수동검증
        result.put("field_stats",      fieldStats);
        result.put("new",              newRows);
        result.put("changed",          changedRows);
        result.put("unverified",       unverifiedRows);
        return result;
    }

    private void syncCheckField(List<Map<String, Object>> changes, String field, String current, String newVal) {
        if (newVal == null) newVal = "";
        if (!current.equals(newVal)) {
            Map<String, Object> c = new java.util.LinkedHashMap<>();
            c.put("field", field); c.put("current", current); c.put("new", newVal);
            changes.add(c);
        }
    }

    /**
     * 확정 스냅샷 기준값. previewSync에서 비교하는 필드들을 정규화해 이어붙인 문자열.
     * 담당자가 "확인함"으로 확정한 시점의 ERP 값과 동일하면 다음 미리보기에서 제외하는 데 사용.
     */
    private String syncRef(Map<String, Object> erpRow) {
        if (erpRow == null) return "";
        String amt = erpRow.get("deduct_amount") != null ? erpRow.get("deduct_amount").toString() : "";
        return String.join("|",
                str(erpRow.get("bank_code")),
                str(erpRow.get("bank_account")),
                str(erpRow.get("member_no") != null ? erpRow.get("member_no") : erpRow.get("erp_member_no")),
                str(erpRow.get("id_number")),
                str(erpRow.get("member_name")),
                amt,
                str(erpRow.get("deduct_day")));
    }

    /** ERP 동기화 선택 반영 — 선택된 cltcd만 INSERT/UPDATE */
    public Map<String, Object> applySync(String spjangcd, List<String> selectedCltcds, String userId) {
        Set<String> selected = (selectedCltcds != null && !selectedCltcds.isEmpty())
                ? new java.util.HashSet<>(selectedCltcds) : null;
        log.info("[ERP반영] 시작 spjangcd={} 선택={}", spjangcd, selected == null ? "전체" : selected.size() + "건");

        Map<String, Object> erp = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT host, port, db_name, username, password, custcd, ms_spjangcd FROM tb_xa012_erp WHERE spjangcd = :spjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd));
        if (erp == null) throw new IllegalStateException("ERP 접속정보가 없습니다.");
        String custcd     = str(erp.get("custcd"));
        String msSpjangcd = str(erp.get("ms_spjangcd"));

        Map<String, Object> cms = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT is_normal_status, amount_round_unit FROM tb_xa012_cms WHERE spjangcd = :spjangcd AND ms_spjangcd IS NOT DISTINCT FROM :msSpjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd)
                        .addValue("msSpjangcd", msSpjangcd.isEmpty() ? null : msSpjangcd));
        if (cms == null || !Boolean.TRUE.equals(cms.get("is_normal_status")))
            throw new IllegalStateException("CMS 서비스 상태가 승인이 아닙니다.");
        int roundUnit = cms.get("amount_round_unit") != null ? ((Number) cms.get("amount_round_unit")).intValue() : 1;

        Set<String> excludeSet = sqlRunner.getRows(/* skip_tenant_check */
                        "SELECT cltcd FROM cms_member_sync_exclude WHERE spjangcd = :spjangcd",
                        new MapSqlParameterSource("spjangcd", spjangcd))
                .stream().map(r -> str(r.get("cltcd"))).collect(java.util.stream.Collectors.toSet());

        String url = String.format("jdbc:sqlserver://%s:%s;databaseName=%s;encrypt=false",
                str(erp.get("host")), str(erp.get("port")), str(erp.get("db_name")));
        try { Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); }
        catch (ClassNotFoundException e) { throw new IllegalStateException("MSSQL 드라이버 없음"); }

        int inserted = 0, updated = 0, skipped = 0, failed = 0;

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                url, str(erp.get("username")), str(erp.get("password")))) {

            for (Map<String, Object> erpRow : fetchErpSyncRows(conn, spjangcd, custcd, roundUnit, excludeSet)) {
                String actcd = str(erpRow.get("sync_key"));
                if (selected != null && !selected.contains(actcd)) { skipped++; continue; }

                try {
                    Map<String, Object> existing = sqlRunner.getRow(/* skip_tenant_check */
                            // 거래처 경로(:mActcd IS NULL)에서 actcd IS NULL 을 강제하면,
                            //  actcd 가 채워진 기존 회원을 못 찾아 신규로 생성해버린다.
                            //  (2026-08-24 6W 60건 중복 생성 사고)
                            //  → cltcd 로 찾되, 같은 cltcd 에 현장 회원이 섞여 있으면
                            //    거래처 회원(actcd 없음)을 우선해 1건만 잡는다.
                            "SELECT id, member_no, _modified FROM cms_member "
                                    + "WHERE spjangcd = :spjangcd AND status <> 'INACTIVE' AND "
                                    + "( (:mActcd IS NOT NULL AND actcd = :mActcd) OR (:mActcd IS NULL AND cltcd = :mCltcd) ) ORDER BY CASE WHEN actcd IS NULL THEN 0 ELSE 1 END, id LIMIT 1",
                            new MapSqlParameterSource("spjangcd", spjangcd)
                                    .addValue("mActcd", erpRow.get("erp_actcd"))
                                    .addValue("mCltcd", erpRow.get("erp_cltcd")));

                    String erpMemberNo = (String) erpRow.get("erp_member_no");
                    String memberNo = (existing != null && StringUtils.hasText(str(existing.get("member_no"))))
                            ? str(existing.get("member_no")) : erpMemberNo;

                    MapSqlParameterSource p = new MapSqlParameterSource();
                    p.addValue("spjangcd",     spjangcd);
                    p.addValue("memberNo",     memberNo);
                    p.addValue("actcd",        erpRow.get("erp_actcd"));
                    p.addValue("cltcd",        erpRow.get("erp_cltcd"));
                    p.addValue("mActcd",       erpRow.get("erp_actcd"));
                    p.addValue("mCltcd",       erpRow.get("erp_cltcd"));
                    // 위에서 찾은 회원 id 로 직접 갱신한다.
                    //  조건식을 UPDATE 에서 다시 평가하면 같은 cltcd 의 다른 회원까지 덮어쓸 수 있다.
                    p.addValue("mid",          existing != null ? existing.get("id") : null);
                    p.addValue("agreeYn",      erpRow.get("agree_yn"));
                    p.addValue("memberName",   erpRow.get("member_name"));
                    p.addValue("memberType",   erpRow.get("member_type"));
                    p.addValue("idNumber",     erpRow.get("id_number"));
                    p.addValue("residentNo",   erpRow.get("resident_no"));
                    p.addValue("bizNo",        erpRow.get("biz_no"));
                    p.addValue("bankCode",     erpRow.get("bank_code"));
                    p.addValue("bankAccount",  erpRow.get("bank_account"));
                    p.addValue("phone",        erpRow.get("phone"));
                    p.addValue("email",        erpRow.get("email"));
                    p.addValue("adresa",       erpRow.get("adresa"));
                    p.addValue("zipcd",        erpRow.get("zipcd"));
                    p.addValue("deductAmount", erpRow.get("deduct_amount"));
                    p.addValue("deductDay",    erpRow.get("deduct_day"));
                    p.addValue("startDate",    erpRow.get("start_date"));
                    p.addValue("endDate",      erpRow.get("end_date"));
                    p.addValue("cycleType",    erpRow.get("cycle_type"));
                    p.addValue("cycleMonths",  erpRow.get("cycle_months"));
                    p.addValue("deductMonthType", erpRow.get("deduct_month_type"));
                    p.addValue("userId",       userId);

                    if (existing == null) {
                        sqlRunner.execute(/* skip_tenant_check */
                                """
                                INSERT INTO cms_member (
                                    spjangcd, member_no, member_type, member_name,
                                    id_number, resident_no, biz_no, bank_code, bank_account,
                                    phone, email, adresa, zipcd,
                                    deduct_amount, deduct_day, start_date, end_date,
                                    cycle_type, cycle_months, deduct_month_type, agree_yn, actcd, cltcd, status,
                                    _creater_id, _created, _modifier_id, _modified
                                ) VALUES (
                                    :spjangcd, :memberNo, :memberType, :memberName,
                                    :idNumber, :residentNo, :bizNo, :bankCode, :bankAccount,
                                    :phone, :email, :adresa, :zipcd,
                                    :deductAmount, :deductDay, :startDate, :endDate,
                                    :cycleType, :cycleMonths, :deductMonthType, :agreeYn, :actcd, :cltcd, 'ACTIVE',
                                    :userId, NOW(), :userId, NOW()
                                )
                                """, p);
                        inserted++;
                    } else {
                        String before = str(existing.get("_modified"));
                        sqlRunner.execute(/* skip_tenant_check */
                                """
                                UPDATE cms_member SET
                                 member_name    = :memberName,    member_type    = :memberType,
                                 member_no      = COALESCE(:memberNo, member_no),
                                 id_number      = :idNumber,      resident_no    = :residentNo,
                                 biz_no         = COALESCE(:bizNo, biz_no),
                                 bank_code      = :bankCode,      bank_account   = :bankAccount,
                                 phone          = :phone,         email          = :email,
                                 adresa         = :adresa,        zipcd          = :zipcd,
                                 deduct_amount  = :deductAmount,  deduct_day     = :deductDay,
                                 start_date     = :startDate,     end_date       = :endDate,
                                 cycle_type     = :cycleType,     cycle_months   = :cycleMonths,
                                 agree_yn       = CASE WHEN :agreeYn='Y' THEN 'Y' ELSE agree_yn END,
                                 actcd          = COALESCE(actcd, :actcd),
                                 cltcd          = COALESCE(cltcd, :cltcd),
                                 _modifier_id   = CASE WHEN (
                                     COALESCE(member_type,'')   != COALESCE(:memberType,'')   OR
                                     COALESCE(member_name,'')   != COALESCE(:memberName,'')   OR
                                     COALESCE(id_number,'')     != COALESCE(:idNumber,'')     OR
                                     COALESCE(resident_no,'')   != COALESCE(:residentNo,'')   OR
                                     COALESCE(biz_no,'')        != COALESCE(:bizNo,'')        OR
                                     COALESCE(bank_code,'')     != COALESCE(:bankCode,'')     OR
                                     COALESCE(bank_account,'')  != COALESCE(:bankAccount,'')  OR
                                     COALESCE(phone,'')         != COALESCE(:phone,'')        OR
                                     COALESCE(email,'')         != COALESCE(:email,'')        OR
                                     COALESCE(adresa,'')        != COALESCE(:adresa,'')       OR
                                     COALESCE(zipcd,'')         != COALESCE(:zipcd,'')        OR
                                     COALESCE(deduct_amount,0)  != COALESCE(:deductAmount,0)  OR
                                     COALESCE(deduct_day,'')    != COALESCE(:deductDay,'')    OR
                                     COALESCE(start_date,'')    != COALESCE(:startDate,'')    OR
                                     COALESCE(end_date,'')      != COALESCE(:endDate,'')      OR
                                     COALESCE(cycle_type,'')    != COALESCE(:cycleType,'')    OR
                                     COALESCE(cycle_months,'')  != COALESCE(:cycleMonths,'')
                                 ) THEN :userId ELSE _modifier_id END,
                                 _modified      = CASE WHEN (
                                     COALESCE(member_type,'')   != COALESCE(:memberType,'')   OR
                                     COALESCE(member_name,'')   != COALESCE(:memberName,'')   OR
                                     COALESCE(id_number,'')     != COALESCE(:idNumber,'')     OR
                                     COALESCE(resident_no,'')   != COALESCE(:residentNo,'')   OR
                                     COALESCE(biz_no,'')        != COALESCE(:bizNo,'')        OR
                                     COALESCE(bank_code,'')     != COALESCE(:bankCode,'')     OR
                                     COALESCE(bank_account,'')  != COALESCE(:bankAccount,'')  OR
                                     COALESCE(phone,'')         != COALESCE(:phone,'')        OR
                                     COALESCE(email,'')         != COALESCE(:email,'')        OR
                                     COALESCE(adresa,'')        != COALESCE(:adresa,'')       OR
                                     COALESCE(zipcd,'')         != COALESCE(:zipcd,'')        OR
                                     COALESCE(deduct_amount,0)  != COALESCE(:deductAmount,0)  OR
                                     COALESCE(deduct_day,'')    != COALESCE(:deductDay,'')    OR
                                     COALESCE(start_date,'')    != COALESCE(:startDate,'')    OR
                                     COALESCE(end_date,'')      != COALESCE(:endDate,'')      OR
                                     COALESCE(cycle_type,'')    != COALESCE(:cycleType,'')    OR
                                     COALESCE(cycle_months,'')  != COALESCE(:cycleMonths,'')
                                 ) THEN NOW() ELSE _modified END
                                WHERE id = :mid
                                """, p);

                        Map<String, Object> after = sqlRunner.getRow(/* skip_tenant_check */
                                "SELECT _modified FROM cms_member WHERE id = :mid",
                                new MapSqlParameterSource("mid", existing.get("id")));
                        if (!before.equals(after != null ? str(after.get("_modified")) : "")) updated++;
                        else skipped++;
                    }
                } catch (Exception e) {
                    log.warn("[ERP반영] 행 처리 실패: actcd={} {}", actcd, e.getMessage());
                    failed++;
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("MSSQL 접속 실패: " + e.getMessage());
        }

        log.info("[ERP반영] 완료 spjangcd={} 신규={} 수정={} 스킵={} 실패={}", spjangcd, inserted, updated, skipped, failed);
        return Map.of("inserted", inserted, "updated", updated, "skipped", skipped, "failed", failed);
    }

    /** 필드별 선택 반영에서 허용하는 필드 → cms_member 컬럼 매핑 */
    private static final Map<String, String> SYNC_FIELD_COL = Map.of(
            "bank_code",     "bank_code",
            "bank_account",  "bank_account",
            "member_no",     "member_no",
            "id_number",     "id_number",
            "member_name",   "member_name",
            "deduct_amount", "deduct_amount",
            "deduct_day",    "deduct_day");

    /**
     * ERP 동기화 필드별 선택 반영 + 확정.
     * 담당자가 회원별로 "어느 필드를 ERP 값으로 바꿀지"를 고른 결과(decisions)를 반영한다.
     *  - erpFields에 든 필드만 ERP 값으로 UPDATE (나머지는 웹 값 유지)
     *  - 반영 여부와 무관하게 sync_confirmed_at/ref를 기록 → 같은 ERP 상태면 다음 미리보기에서 제외
     *
     * decisions: [{ "cltcd": "...", "erpFields": ["bank_account","deduct_amount"] }, ...]
     *            erpFields가 비어 있으면 "웹 값 유지(확인만)" 로 처리.
     */
    public Map<String, Object> applySyncSelective(String spjangcd, List<Map<String, Object>> decisions, String userId) {
        if (decisions == null || decisions.isEmpty()) {
            return Map.of("updated", 0, "confirmed", 0, "failed", 0);
        }
        log.info("[ERP선택반영] 시작 spjangcd={} 대상={}건", spjangcd, decisions.size());

        Map<String, Object> erp = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT host, port, db_name, username, password, custcd, ms_spjangcd FROM tb_xa012_erp WHERE spjangcd = :spjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd));
        if (erp == null) throw new IllegalStateException("ERP 접속정보가 없습니다.");
        String custcd     = str(erp.get("custcd"));
        String msSpjangcd = str(erp.get("ms_spjangcd"));

        Map<String, Object> cms = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT is_normal_status, amount_round_unit FROM tb_xa012_cms WHERE spjangcd = :spjangcd AND ms_spjangcd IS NOT DISTINCT FROM :msSpjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd)
                        .addValue("msSpjangcd", msSpjangcd.isEmpty() ? null : msSpjangcd));
        int roundUnit = (cms != null && cms.get("amount_round_unit") != null)
                ? ((Number) cms.get("amount_round_unit")).intValue() : 1;

        Set<String> excludeSet = sqlRunner.getRows(/* skip_tenant_check */
                        "SELECT cltcd FROM cms_member_sync_exclude WHERE spjangcd = :spjangcd",
                        new MapSqlParameterSource("spjangcd", spjangcd))
                .stream().map(r -> str(r.get("cltcd"))).collect(java.util.stream.Collectors.toSet());

        // 결정 대상 cltcd 집합
        Set<String> wanted = decisions.stream().map(d -> str(d.get("cltcd")))
                .filter(StringUtils::hasText).collect(java.util.stream.Collectors.toSet());

        String url = String.format("jdbc:sqlserver://%s:%s;databaseName=%s;encrypt=false",
                str(erp.get("host")), str(erp.get("port")), str(erp.get("db_name")));
        try { Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); }
        catch (ClassNotFoundException e) { throw new IllegalStateException("MSSQL 드라이버 없음"); }

        // ERP 행 인덱싱. 키는 미리보기가 화면에 내려준 값(sync_key)과 동일해야 한다.
        //  실제 회원 매칭은 아래에서 erp_actcd/erp_cltcd 로 필드별로 수행한다.
        Map<String, Map<String, Object>> erpRowMap = new java.util.HashMap<>();
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                url, str(erp.get("username")), str(erp.get("password")))) {
            for (Map<String, Object> r : fetchErpSyncRows(conn, spjangcd, custcd, roundUnit, excludeSet)) {
                String c = str(r.get("sync_key"));
                if (wanted.contains(c)) erpRowMap.put(c, r);
            }
        } catch (Exception e) {
            throw new IllegalStateException("MSSQL 접속 실패: " + e.getMessage());
        }

        int updated = 0, confirmed = 0, failed = 0;
        for (Map<String, Object> d : decisions) {
            String cltcd = str(d.get("cltcd"));
            if (!StringUtils.hasText(cltcd)) continue;
            Map<String, Object> erpRow = erpRowMap.get(cltcd);
            if (erpRow == null) { failed++; continue; } // ERP에서 사라졌거나 대상 아님

            @SuppressWarnings("unchecked")
            List<String> erpFields = d.get("erpFields") instanceof List
                    ? (List<String>) d.get("erpFields") : java.util.Collections.emptyList();

            try {
                Map<String, Object> existing = sqlRunner.getRow(/* skip_tenant_check */
                        // 거래처 경로는 cltcd 로 찾고, 같은 cltcd 에 현장 회원이 있으면
                        //  거래처 회원(actcd 없음)을 우선한다. (중복 생성 방지)
                        "SELECT id FROM cms_member "
                                + "WHERE spjangcd = :spjangcd AND status <> 'INACTIVE' AND "
                                + "( (:mActcd IS NOT NULL AND actcd = :mActcd) OR (:mActcd IS NULL AND cltcd = :mCltcd) ) ORDER BY CASE WHEN actcd IS NULL THEN 0 ELSE 1 END, id LIMIT 1",
                        new MapSqlParameterSource("spjangcd", spjangcd)
                                .addValue("mActcd", erpRow.get("erp_actcd"))
                                .addValue("mCltcd", erpRow.get("erp_cltcd")));

                // 신규 회원: cms_member에 없으면 ERP 값 통짜 INSERT (필드선택 무의미)
                if (existing == null) {
                    String memberNo = StringUtils.hasText(str(erpRow.get("member_no")))
                            ? str(erpRow.get("member_no")) : str(erpRow.get("erp_member_no"));
                    MapSqlParameterSource ip = new MapSqlParameterSource();
                    ip.addValue("spjangcd",     spjangcd);
                    ip.addValue("memberNo",     memberNo);
                    ip.addValue("memberType",   erpRow.get("member_type"));
                    ip.addValue("memberName",   erpRow.get("member_name"));
                    ip.addValue("idNumber",     erpRow.get("id_number"));
                    ip.addValue("residentNo",   erpRow.get("resident_no"));
                    ip.addValue("bizNo",        erpRow.get("biz_no"));
                    ip.addValue("bankCode",     erpRow.get("bank_code"));
                    ip.addValue("bankAccount",  resolveRecommendedAccount(spjangcd, null, erpRow));
                    ip.addValue("deductAmount", erpRow.get("deduct_amount"));
                    ip.addValue("deductDay",    erpRow.get("deduct_day"));
                    ip.addValue("agreeYn",      erpRow.get("agree_yn"));
                    ip.addValue("confRef",      syncRef(erpRow));
                    ip.addValue("actcd",        erpRow.get("erp_actcd"));
                    ip.addValue("cltcd",        erpRow.get("erp_cltcd"));
                    ip.addValue("monthType",    erpRow.get("deduct_month_type"));
                    ip.addValue("userId",       userId);
                    sqlRunner.execute(/* skip_tenant_check */
                            """
                            INSERT INTO cms_member (
                                spjangcd, member_no, member_type, member_name, id_number, resident_no, biz_no,
                                bank_code, bank_account, deduct_amount, deduct_day, deduct_month_type, agree_yn, actcd, cltcd, status,
                                sync_confirmed_at, sync_confirmed_ref,
                                _creater_id, _created, _modifier_id, _modified
                            ) VALUES (
                                :spjangcd, :memberNo, :memberType, :memberName, :idNumber, :residentNo, :bizNo,
                                :bankCode, :bankAccount, :deductAmount, :deductDay, :monthType, :agreeYn, :actcd, :cltcd, 'ACTIVE',
                                NOW(), :confRef,
                                :userId, NOW(), :userId, NOW()
                            )
                            """, ip);
                    updated++;
                    continue;
                }

                // 선택된 필드만 SET 절 구성 (화이트리스트로 컬럼 검증)
                StringBuilder set = new StringBuilder();
                MapSqlParameterSource p = new MapSqlParameterSource();
                p.addValue("spjangcd", spjangcd);
                p.addValue("actcd",    erpRow.get("erp_actcd"));
                p.addValue("cltcd",    erpRow.get("erp_cltcd"));
                p.addValue("mActcd",   erpRow.get("erp_actcd"));
                p.addValue("mCltcd",   erpRow.get("erp_cltcd"));
                p.addValue("mid",      existing.get("id"));
                p.addValue("userId",   userId);

                // 계좌 반영 시 previewSync와 동일한 추천계좌(실패계좌 배제 + XCLIENT 우선)를 사용.
                //  화면에 보여준 값(추천)과 실제 반영값이 일치해야 하므로 여기서도 동일 계산.
                Long emId = ((Number) existing.get("id")).longValue();
                String recAcc = resolveRecommendedAccount(spjangcd, emId, erpRow);

                for (String f : erpFields) {
                    String col = SYNC_FIELD_COL.get(f);
                    if (col == null) continue; // 허용되지 않은 필드 무시
                    Object val;
                    if ("member_no".equals(f)) {
                        val = StringUtils.hasText(str(erpRow.get("member_no")))
                                ? erpRow.get("member_no") : erpRow.get("erp_member_no");
                    } else if ("bank_account".equals(f)) {
                        val = recAcc;               // ★ EB13 계좌가 아니라 추천계좌
                    } else {
                        val = erpRow.get(f);
                    }
                    set.append(col).append(" = :").append(f).append(", ");
                    p.addValue(f, val);
                }
                // 확정 스냅샷은 항상 기록 (웹 유지여도 다음 미리보기에서 제외되도록)
                p.addValue("confRef", syncRef(erpRow));
                String sql = "UPDATE cms_member SET " + set
                        + "sync_confirmed_at = NOW(), sync_confirmed_ref = :confRef, "
                        + "actcd = COALESCE(actcd, :actcd), cltcd = COALESCE(cltcd, :cltcd), "
                        + "_modifier_id = :userId, "
                        + "_modified = CASE WHEN :hasField THEN NOW() ELSE _modified END "
                        + "WHERE id = :mid";
                p.addValue("hasField", !erpFields.isEmpty());
                sqlRunner.execute(/* skip_tenant_check */ sql, p);

                if (erpFields.isEmpty()) confirmed++; else updated++;
            } catch (Exception e) {
                log.warn("[ERP선택반영] 실패 cltcd={} {}", cltcd, e.getMessage());
                failed++;
            }
        }

        log.info("[ERP선택반영] 완료 spjangcd={} 반영={} 확인만={} 실패={}", spjangcd, updated, confirmed, failed);
        return Map.of("updated", updated, "confirmed", confirmed, "failed", failed);
    }

    /**
     * 추천 계좌 결정 — previewSync와 동일 규칙.
     *  "cms_billing에서 계좌문제(0017/0019 등)로 실패한 계좌를 배제 + XCLIENT(원장) 우선".
     *  memberId가 null(신규)이면 실패이력이 없으므로 XCLIENT>EB13 순.
     */
    private String resolveRecommendedAccount(String spjangcd, Long memberId, Map<String, Object> erpRow) {
        String eb13Acc    = str(erpRow.get("bank_account")).replaceAll("[^0-9]", "");
        String xclientAcc = str(erpRow.get("xclient_account")).replaceAll("[^0-9]", "");

        String failedAcc = null;
        if (memberId != null) {
            Map<String, Object> fb = sqlRunner.getRow(/* skip_tenant_check */
                    """
                    SELECT bank_account, result_code
                    FROM cms_billing
                    WHERE spjangcd = :spjangcd AND member_id = :memberId
                    ORDER BY deduct_date DESC NULLS LAST, _created DESC NULLS LAST
                    LIMIT 1
                    """,
                    new MapSqlParameterSource("spjangcd", spjangcd).addValue("memberId", memberId));
            if (fb != null) {
                String rc = str(fb.get("result_code"));
                java.util.Set<String> acctFailCodes = java.util.Set.of("0017", "0019", "0012", "0013", "0014");
                if (acctFailCodes.contains(rc)) {
                    failedAcc = str(fb.get("bank_account")).replaceAll("[^0-9]", "");
                    if (failedAcc.isEmpty()) failedAcc = null;
                }
            }
        }

        // ★ 현장 단위 회원(erp_actcd 보유)은 XCLIENT(거래처 원장) 계좌를 쓰면 안 된다.
        //   현장의 xclient_account 는 '소속 거래처'의 계좌이므로, 같은 거래처의 다른 현장·
        //   거래처 회원과 계좌가 동일해져 엉뚱한 곳에서 출금된다.
        //   (CS메디컬프라자가 지원에셋플러스 계좌로 등록된 사고)
        //   현장은 자기 계약(E101)/자기 EB13 등록분인 eb13Acc 를 우선한다.
        boolean isSite = StringUtils.hasText(str(erpRow.get("erp_actcd")));
        String[] order = isSite ? new String[]{ eb13Acc, xclientAcc }
                : new String[]{ xclientAcc, eb13Acc };

        for (String cand : order) {
            if (cand == null || cand.isEmpty()) continue;
            if (failedAcc != null && cand.equals(failedAcc)) continue;
            return cand;
        }
        // 후보가 다 실패계좌뿐이면 위와 같은 우선순위로라도 채운다.
        for (String cand : order) {
            if (cand != null && !cand.isEmpty()) return cand;
        }
        return "";
    }

    /**
     * ERP ↔ cms_member 동기화 오염 진단 (읽기 전용 — UPDATE 없음)
     *
     * EB13 대조 후 cms_billing 최근 결과코드로 교차검증:
     *  - 성공계열 (0000/0021)            → "성공중(오염아님)"
     *  - 값문제실패 (0017/0012/0013/0014) → "진짜오염"
     *  - 청구이력 없음                    → "검증불가(미청구)"
     *
     * STEP 1 검증 3건(114631·161305·130437)은 반드시 일치로 나와야 한다.
     */
    public Map<String, Object> diagnoseSync(String spjangcd) {
        log.info("[ERP진단] 시작 spjangcd={}", spjangcd);

        // 1. ERP 접속정보
        Map<String, Object> erp = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT host, port, db_name, username, password, custcd FROM tb_xa012_erp WHERE spjangcd = :spjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd));
        if (erp == null) throw new IllegalStateException("ERP 접속정보가 없습니다.");
        String custcd = str(erp.get("custcd"));
        if (custcd.isEmpty()) throw new IllegalStateException("업체코드(custcd)가 없습니다.");

        // 2. cms_member 현재값 로드 (id 포함)
        List<Map<String, Object>> members = sqlRunner.getRows(/* skip_tenant_check */
                "SELECT id, cltcd, actcd, member_no, member_name, id_number, bank_code, bank_account, agree_yn "
                        + "FROM cms_member WHERE spjangcd = :spjangcd AND status <> 'INACTIVE' "
                        + "ORDER BY COALESCE(actcd, cltcd)",  // 표시 정렬용
                new MapSqlParameterSource("spjangcd", spjangcd));

        Map<String, Map<String, Object>> memberMap = new java.util.LinkedHashMap<>();
        // TB_CMSEB13.CLTCD 는 ①경로에서 E601.actcd 와 조인되므로 실제로는 actcd 값이고,
        //  ②경로(ACTCD 비어있음)에서는 XCLIENT.cltcd 다. 따라서 양쪽 다 키로 등록하되
        //  번호대 충돌 시 actcd 가 이기도록 actcd 를 나중에 덮어쓴다.
        for (Map<String, Object> m : members) {
            String c = str(m.get("cltcd"));
            if (StringUtils.hasText(c)) memberMap.put(c, m);
        }
        for (Map<String, Object> m : members) {
            String a = str(m.get("actcd"));
            if (StringUtils.hasText(a)) memberMap.put(a, m);
        }

        // 3. 최근 billing result_code per member_id (DISTINCT ON: PostgreSQL)
        List<Map<String, Object>> billingRows = sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT DISTINCT ON (b.member_id) b.member_id, b.result_code
                FROM cms_billing b
                WHERE b.spjangcd = :spjangcd
                ORDER BY b.member_id,
                    b.deduct_date DESC NULLS LAST,
                    b._created DESC NULLS LAST
                """,
                new MapSqlParameterSource("spjangcd", spjangcd));

        Map<Long, String> billingMap = new java.util.HashMap<>();
        for (Map<String, Object> br : billingRows) {
            Object mid = br.get("member_id");
            if (mid != null) billingMap.put(((Number) mid).longValue(), str(br.get("result_code")));
        }

        java.util.Set<String> successCodes = java.util.Set.of("0000", "0021");
        java.util.Set<String> failureCodes = java.util.Set.of("0017", "0012", "0013", "0014");

        // 4. MSSQL 연결
        String url = String.format("jdbc:sqlserver://%s:%s;databaseName=%s;encrypt=false",
                str(erp.get("host")), str(erp.get("port")), str(erp.get("db_name")));
        try { Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); }
        catch (ClassNotFoundException e) { throw new IllegalStateException("MSSQL 드라이버 없음"); }

        List<Map<String, Object>> bankCodeMismatch    = new java.util.ArrayList<>();
        List<Map<String, Object>> bankAccountMismatch = new java.util.ArrayList<>();
        List<Map<String, Object>> payerMismatch       = new java.util.ArrayList<>();
        List<Map<String, Object>> idNumberMismatch    = new java.util.ArrayList<>();
        List<Map<String, Object>> noEb13              = new java.util.ArrayList<>();
        List<Map<String, Object>> abnormalPayer       = new java.util.ArrayList<>();
        List<Map<String, Object>> realContamination   = new java.util.ArrayList<>();

        // per-category 집계: [0]진짜오염 [1]성공중 [2]검증불가 [3]기타실패
        int[] bankCodeCounts    = {0, 0, 0, 0};
        int[] bankAccountCounts = {0, 0, 0, 0};
        int[] payerCounts       = {0, 0, 0, 0};
        int[] idNumberCounts    = {0, 0, 0, 0};

        Map<String, Map<String, Object>> eb13Map = new java.util.LinkedHashMap<>();

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                url, str(erp.get("username")), str(erp.get("password")))) {

            // 4a. 이상 등록건: ENDFLAG='Y' 이지만 BANKCLTCD가 납부자번호 형태 아님
            String abnSql =
                    "SELECT DISTINCT CLTCD, BANKCLTCD FROM TB_CMSEB13 WITH(NOLOCK)" +
                            " WHERE CUSTCD = ? AND ENDFLAG = 'Y'" +
                            " AND (PATINDEX('%[^0-9]%', LTRIM(RTRIM(BANKCLTCD))) > 0" +
                            "   OR LEN(LTRIM(RTRIM(BANKCLTCD))) NOT BETWEEN 10 AND 13)";
            try (java.sql.PreparedStatement ps = conn.prepareStatement(abnSql)) {
                ps.setString(1, custcd);
                try (java.sql.ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String cltcd = rs.getString("CLTCD");
                        if (!memberMap.containsKey(cltcd)) continue;
                        Map<String, Object> row = new java.util.LinkedHashMap<>();
                        row.put("cltcd", cltcd);
                        row.put("member_name", str(memberMap.get(cltcd).get("member_name")));
                        row.put("raw_bankcltcd", rs.getString("BANKCLTCD"));
                        abnormalPayer.add(row);
                    }
                }
            }

            // 4b. 유효 최신 등록건: CLTCD 단일매칭, ENDFLAG='Y', 정상 BANKCLTCD, SPDATE 최신
            String eb13Sql =
                    "SELECT EB.CLTCD," +
                            " LTRIM(RTRIM(EB.BANKCLTCD)) AS payer_no," +
                            " EB.BANKCD AS erp_bank_code," +
                            " LTRIM(RTRIM(COALESCE(XB.bnkcode, ''))) AS cms_bank_code," +
                            " REPLACE(REPLACE(LTRIM(RTRIM(COALESCE(EB.CMSACCNUM, ''))), '-', ''), ' ', '') AS bank_account," +
                            " LTRIM(RTRIM(COALESCE(EB.SAUPNUM, ''))) AS id_number," +
                            " EB.SPDATE" +
                            " FROM TB_CMSEB13 EB WITH(NOLOCK)" +
                            " LEFT JOIN TB_XBANK XB WITH(NOLOCK) ON EB.BANKCD = XB.bankcd" +
                            " WHERE EB.CUSTCD = ?" +
                            " AND EB.ENDFLAG = 'Y'" +
                            " AND PATINDEX('%[^0-9]%', LTRIM(RTRIM(EB.BANKCLTCD))) = 0" +
                            " AND LEN(LTRIM(RTRIM(EB.BANKCLTCD))) BETWEEN 10 AND 13" +
                            " AND EB.SPDATE = (" +
                            "   SELECT MAX(SPDATE) FROM TB_CMSEB13 WITH(NOLOCK)" +
                            "   WHERE CLTCD = EB.CLTCD AND CUSTCD = EB.CUSTCD AND ENDFLAG = 'Y'" +
                            "   AND PATINDEX('%[^0-9]%', LTRIM(RTRIM(BANKCLTCD))) = 0" +
                            "   AND LEN(LTRIM(RTRIM(BANKCLTCD))) BETWEEN 10 AND 13" +
                            " )";
            try (java.sql.PreparedStatement ps = conn.prepareStatement(eb13Sql)) {
                ps.setString(1, custcd);
                try (java.sql.ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        String cltcd = rs.getString("CLTCD");
                        if (eb13Map.containsKey(cltcd)) continue; // SPDATE 동일 중복은 첫 번째
                        Map<String, Object> row = new java.util.LinkedHashMap<>();
                        row.put("cltcd", cltcd);
                        row.put("payer_no",      str(rs.getString("payer_no")));
                        row.put("erp_bank_code", str(rs.getString("erp_bank_code")));
                        row.put("cms_bank_code", str(rs.getString("cms_bank_code")));
                        row.put("bank_account",  str(rs.getString("bank_account")));
                        row.put("id_number",     str(rs.getString("id_number")));
                        row.put("spdate",        str(rs.getString("SPDATE")));
                        eb13Map.put(cltcd, row);
                    }
                }
            }

        } catch (Exception e) {
            throw new IllegalStateException("MSSQL 접속 실패: " + e.getMessage());
        }

        // 5. 대조 및 분류
        int matchCount     = 0;
        int protectedCount = 0; // 청구 성공 중 → EB13 비교 생략
        java.util.Set<String> verifySet = java.util.Set.of("114631", "161305", "130437");
        Map<String, Map<String, Object>> verifyRecords = new java.util.LinkedHashMap<>();

        for (Map.Entry<String, Map<String, Object>> entry : memberMap.entrySet()) {
            String cltcd  = entry.getKey();
            Map<String, Object> member = entry.getValue();

            String memberNo   = str(member.get("member_no")).trim();
            String memberName = str(member.get("member_name"));
            String idNumber   = str(member.get("id_number")).replaceAll("[-\\s]", "").trim();
            String bankCode   = str(member.get("bank_code")).trim();
            String bankAcc    = str(member.get("bank_account")).replaceAll("[-\\s]", "").trim();

            Object midObj = member.get("id");
            Long memberId = midObj != null ? ((Number) midObj).longValue() : null;
            String lastResultCode = memberId != null ? billingMap.get(memberId) : null;

            // 청구 성공 중 → 현재값이 실제 작동값, EB13 비교 생략
            if (lastResultCode != null && successCodes.contains(lastResultCode)) {
                protectedCount++;
                if (verifySet.contains(cltcd))
                    verifyRecords.put(cltcd, diagVerifyRow(cltcd, memberName, "성공중(보호)", lastResultCode));
                continue;
            }

            String billingClass;
            if (lastResultCode == null)                    billingClass = "검증불가(미청구)";
            else if (failureCodes.contains(lastResultCode)) billingClass = "진짜오염";
            else                                            billingClass = "기타실패(" + lastResultCode + ")";

            Map<String, Object> eb13 = eb13Map.get(cltcd);
            if (eb13 == null) {
                Map<String, Object> row = new java.util.LinkedHashMap<>();
                row.put("cltcd", cltcd);
                row.put("member_name", memberName);
                row.put("member_no", memberNo);
                row.put("agree_yn", str(member.get("agree_yn")));
                row.put("last_result_code", lastResultCode);
                noEb13.add(row);
                if (verifySet.contains(cltcd))
                    verifyRecords.put(cltcd, diagVerifyRow(cltcd, memberName, "EB13없음", lastResultCode));
                continue;
            }

            String eb13Payer    = str(eb13.get("payer_no"));
            String eb13BankCode = str(eb13.get("cms_bank_code"));
            String eb13BankAcc  = str(eb13.get("bank_account"));
            String eb13IdNum    = str(eb13.get("id_number")).replaceAll("[-\\s]", "");
            String spdate       = str(eb13.get("spdate"));

            boolean allMatch = true;

            if (!bankCode.equals(eb13BankCode)) {
                Map<String, Object> row = new java.util.LinkedHashMap<>();
                row.put("cltcd", cltcd);                      row.put("member_name", memberName);
                row.put("current_bank_code", bankCode);       row.put("eb13_bank_code", eb13BankCode);
                row.put("erp_bank_code", str(eb13.get("erp_bank_code")));
                row.put("spdate", spdate);
                row.put("last_result_code", lastResultCode);  row.put("billing_classification", billingClass);
                bankCodeMismatch.add(row);
                diagCountUp(bankCodeCounts, billingClass);
                if ("진짜오염".equals(billingClass))
                    realContamination.add(diagRealRow(cltcd, memberName, "bank_code", bankCode, eb13BankCode, lastResultCode));
                allMatch = false;
            }
            if (!bankAcc.equals(eb13BankAcc)) {
                Map<String, Object> row = new java.util.LinkedHashMap<>();
                row.put("cltcd", cltcd);                      row.put("member_name", memberName);
                row.put("current_bank_account", bankAcc);     row.put("eb13_bank_account", eb13BankAcc);
                row.put("current_bank_code", bankCode);       row.put("eb13_bank_code", eb13BankCode);
                row.put("spdate", spdate);
                row.put("last_result_code", lastResultCode);  row.put("billing_classification", billingClass);
                bankAccountMismatch.add(row);
                diagCountUp(bankAccountCounts, billingClass);
                if ("진짜오염".equals(billingClass))
                    realContamination.add(diagRealRow(cltcd, memberName, "bank_account", bankAcc, eb13BankAcc, lastResultCode));
                allMatch = false;
            }
            if (!memberNo.equals(eb13Payer)) {
                Map<String, Object> row = new java.util.LinkedHashMap<>();
                row.put("cltcd", cltcd);                      row.put("member_name", memberName);
                row.put("current_payer", memberNo);           row.put("eb13_payer", eb13Payer);
                row.put("spdate", spdate);
                row.put("last_result_code", lastResultCode);  row.put("billing_classification", billingClass);
                payerMismatch.add(row);
                diagCountUp(payerCounts, billingClass);
                if ("진짜오염".equals(billingClass))
                    realContamination.add(diagRealRow(cltcd, memberName, "payer", memberNo, eb13Payer, lastResultCode));
                allMatch = false;
            }
            if (!idNumber.equals(eb13IdNum)) {
                Map<String, Object> row = new java.util.LinkedHashMap<>();
                row.put("cltcd", cltcd);                      row.put("member_name", memberName);
                row.put("current_id_number", idNumber);       row.put("eb13_id_number", eb13IdNum);
                row.put("spdate", spdate);
                row.put("last_result_code", lastResultCode);  row.put("billing_classification", billingClass);
                idNumberMismatch.add(row);
                diagCountUp(idNumberCounts, billingClass);
                if ("진짜오염".equals(billingClass))
                    realContamination.add(diagRealRow(cltcd, memberName, "id_number", idNumber, eb13IdNum, lastResultCode));
                allMatch = false;
            }

            if (allMatch) {
                matchCount++;
                if (verifySet.contains(cltcd))
                    verifyRecords.put(cltcd, diagVerifyRow(cltcd, memberName, "정상(일치)", lastResultCode));
            } else {
                if (verifySet.contains(cltcd))
                    verifyRecords.put(cltcd, diagVerifyRow(cltcd, memberName, "불일치-" + billingClass, lastResultCode));
            }
        }

        log.info("[ERP진단] 완료 spjangcd={} 전체={} EB13유효={} 성공중보호={} EB13일치={} 진짜오염={}",
                spjangcd, members.size(), eb13Map.size(), protectedCount, matchCount, realContamination.size());
        for (String vc : java.util.List.of("114631", "161305", "130437")) {
            Map<String, Object> vr = verifyRecords.get(vc);
            if (vr != null) log.info("[ERP진단][검증] cltcd={} classification={} last_result_code={}",
                    vc, vr.get("classification"), vr.get("last_result_code"));
            else            log.info("[ERP진단][검증] cltcd={} → cms_member에 없음", vc);
        }

        Map<String, Object> result = new java.util.LinkedHashMap<>();
        result.put("spjangcd",          spjangcd);
        result.put("total_members",     members.size());
        result.put("total_eb13_valid",  eb13Map.size());
        result.put("protected_count",   protectedCount); // 청구 성공 중 → EB13 비교 생략
        result.put("match_count",       matchCount);     // EB13 기준 일치
        // per-category 요약 (전체 = 진짜오염 + 성공중 + 검증불가 + 기타실패)
        result.put("bank_code_mismatch_count",       bankCodeMismatch.size());
        result.put("bank_code_진짜오염",             bankCodeCounts[0]);
        result.put("bank_code_성공중",               bankCodeCounts[1]);
        result.put("bank_code_검증불가",             bankCodeCounts[2]);
        result.put("bank_account_mismatch_count",    bankAccountMismatch.size());
        result.put("bank_account_진짜오염",          bankAccountCounts[0]);
        result.put("bank_account_성공중",            bankAccountCounts[1]);
        result.put("bank_account_검증불가",          bankAccountCounts[2]);
        result.put("payer_mismatch_count",           payerMismatch.size());
        result.put("payer_진짜오염",                 payerCounts[0]);
        result.put("payer_성공중",                   payerCounts[1]);
        result.put("payer_검증불가",                 payerCounts[2]);
        result.put("id_number_mismatch_count",       idNumberMismatch.size());
        result.put("id_number_진짜오염",             idNumberCounts[0]);
        result.put("id_number_성공중",               idNumberCounts[1]);
        result.put("id_number_검증불가",             idNumberCounts[2]);
        result.put("no_eb13_count",               noEb13.size());
        result.put("abnormal_payer_count",        abnormalPayer.size());
        // 진짜오염 통합 (cltcd, member_name, field, current_value, eb13_value, last_result_code)
        result.put("real_contamination_count",    realContamination.size());
        result.put("real_contamination",          realContamination);
        // 검증 3건 상태
        result.put("verification_records",        verifyRecords);
        // 전체 상세 목록 (billing 분류 포함)
        result.put("bank_code_mismatch",          bankCodeMismatch);
        result.put("bank_account_mismatch",       bankAccountMismatch);
        result.put("payer_mismatch",              payerMismatch);
        result.put("id_number_mismatch",          idNumberMismatch);
        result.put("no_eb13",                     noEb13);
        result.put("abnormal_payer",              abnormalPayer);
        return result;
    }

    private void diagCountUp(int[] counts, String billingClass) {
        if ("진짜오염".equals(billingClass))              counts[0]++;
        else if ("성공중(오염아님)".equals(billingClass))  counts[1]++;
        else if ("검증불가(미청구)".equals(billingClass))  counts[2]++;
        else                                              counts[3]++;
    }

    private Map<String, Object> diagRealRow(String cltcd, String memberName, String field,
                                            String currentValue, String eb13Value, String lastResultCode) {
        Map<String, Object> row = new java.util.LinkedHashMap<>();
        row.put("cltcd", cltcd);
        row.put("member_name", memberName);
        row.put("field", field);
        row.put("current_value", currentValue);
        row.put("eb13_value", eb13Value);
        row.put("last_result_code", lastResultCode);
        return row;
    }

    private Map<String, Object> diagVerifyRow(String cltcd, String memberName,
                                              String classification, String lastResultCode) {
        Map<String, Object> row = new java.util.LinkedHashMap<>();
        row.put("cltcd", cltcd);
        row.put("member_name", memberName);
        row.put("classification", classification);
        row.put("last_result_code", lastResultCode);
        return row;
    }

    /** unit 단위 반올림 (1=원단위, 10=십원단위). 0.5는 올림. */
    private long roundAmount(double v, int unit) {
        if (unit <= 1) return Math.round(v);
        return Math.round(v / unit) * (long) unit;
    }

    private String cleanDate(String date) {
        if (date == null || date.trim().isEmpty()) return null;
        String cleaned = date.replaceAll("[-.]", "").trim();
        // 6자리면 20XX로 변환
        if (cleaned.length() == 6) cleaned = "20" + cleaned;
        return cleaned.length() >= 8 ? cleaned.substring(0, 8) : cleaned;
    }

    public Map<String, Object> cancelMember(Long memberId, String userId) {
        String spjangcd = TenantContext.get();

        Map<String, Object> member = sqlRunner.getRow(/* skip_tenant_check */
                """
                SELECT id, member_no, bank_code, bank_account, id_number, member_type,
                       member_name, status
                FROM cms_member
                WHERE id = :memberId AND spjangcd = :spjangcd
                """,
                new MapSqlParameterSource("memberId", memberId).addValue("spjangcd", spjangcd));

        if (member == null) throw new IllegalArgumentException("회원을 찾을 수 없습니다.");
        if (!"ACTIVE".equals(str(member.get("status"))))
            throw new IllegalStateException("ACTIVE 상태 회원만 해지 가능. 현재: " + str(member.get("status")));

        // ★ 중복 신청 방지: 진행 중(PENDING)인 등록/해지 신청이 있으면 막는다.
        Map<String, Object> pendingRow = sqlRunner.getRow(/* skip_tenant_check */
                """
                SELECT COUNT(*) AS cnt FROM cms_account_register
                WHERE spjangcd = :spjangcd AND member_id = :memberId AND status = 'PENDING'
                """,
                new MapSqlParameterSource("memberId", memberId).addValue("spjangcd", spjangcd));
        long pendingCnt = pendingRow != null ? ((Number) pendingRow.get("cnt")).longValue() : 0;
        if (pendingCnt > 0) {
            throw new IllegalStateException(
                    "이미 진행 중인 등록/해지 신청이 있습니다. '출금이체 인증 관리'에서 먼저 처리한 뒤 다시 시도하세요.");
        }

        // 해지 신청 행 INSERT (신규 이력은 그대로 두고 별도 행)
        Map<String, Object> regRow = sqlRunner.getRow(/* skip_tenant_check */
                """
                INSERT INTO cms_account_register (
                    spjangcd, member_id, member_name, member_no,
                    bank_code, bank_account, id_number, member_type,
                    apply_type, apply_date, ei13_status, eb13_status, status,
                    _creater_id, _created, _modifier_id, _modified
                ) VALUES (
                    :spjangcd, :memberId, :memberName, :memberNo,
                    :bankCode, :bankAccount, :idNumber, :memberType,
                    '3', TO_CHAR(NOW(),'YYYYMMDD'), 'SENT', 'PENDING', 'PENDING',
                    :userId, NOW(), :userId, NOW()
                ) RETURNING id
                """,
                new MapSqlParameterSource()
                        .addValue("spjangcd",    spjangcd)
                        .addValue("memberId",    memberId)
                        .addValue("memberName",  str(member.get("member_name")))
                        .addValue("memberNo",    str(member.get("member_no")))
                        .addValue("bankCode",    str(member.get("bank_code")))
                        .addValue("bankAccount", str(member.get("bank_account")))
                        .addValue("idNumber",    str(member.get("id_number")))
                        .addValue("memberType",  str(member.get("member_type")))
                        .addValue("userId",      userId));

        long registerId = ((Number) regRow.get("id")).longValue();

        // 해지행은 PENDING 상태로만 생성. 실제 EB13 전송은 '출금이체 인증 관리'의
        // "인증 등록 신청"에서 신규 건과 함께 하루 1파일로 묶어 보낸다.
        sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_member SET status='PENDING_CANCEL',
                    _modifier_id=:userId, _modified=NOW()
                WHERE id=:memberId AND spjangcd=:spjangcd
                """,
                new MapSqlParameterSource("memberId", memberId).addValue("spjangcd", spjangcd).addValue("userId", userId));
        return Map.of("success", true,
                "message", "해지 신청이 접수되었습니다. '출금이체 인증 관리'에서 인증 등록 신청으로 전송하세요.");
    }

    /**
     * 다건 해지 — 여러 회원을 한 번에 해지 신청.
     * 각 회원마다 해지 레지스터 행(apply_type='3')을 만들고,
     * 유효한 건들을 하나의 EB13 파일로 묶어 전송한 뒤 PENDING_CANCEL로 일괄 반영.
     */
    public Map<String, Object> cancelMembers(List<Long> memberIds, String userId) {
        String spjangcd = TenantContext.get();

        List<Map<String, Object>> skipped = new ArrayList<>();   // 해지 불가(상태 등)
        List<Long> okMemberIds  = new ArrayList<>();             // 전송 성공 대상
        List<Long> okRegisterIds = new ArrayList<>();            // 생성된 해지행

        for (Long memberId : memberIds) {
            Map<String, Object> member = sqlRunner.getRow(/* skip_tenant_check */
                    """
                    SELECT id, member_no, bank_code, bank_account, id_number, member_type,
                           member_name, status
                    FROM cms_member
                    WHERE id = :memberId AND spjangcd = :spjangcd
                    """,
                    new MapSqlParameterSource("memberId", memberId).addValue("spjangcd", spjangcd));

            if (member == null) {
                skipped.add(Map.of("member_id", memberId, "reason", "회원 없음"));
                continue;
            }
            if (!"ACTIVE".equals(str(member.get("status")))) {
                skipped.add(Map.of(
                        "member_id", memberId,
                        "member_name", str(member.get("member_name")),
                        "reason", "ACTIVE 아님(" + str(member.get("status")) + ")"));
                continue;
            }

            // ★ 중복 신청 방지: 진행 중(PENDING) 신청 있으면 이 회원은 건너뜀
            Map<String, Object> pRow = sqlRunner.getRow(/* skip_tenant_check */
                    "SELECT COUNT(*) AS cnt FROM cms_account_register" +
                            " WHERE spjangcd = :spjangcd AND member_id = :memberId AND status = 'PENDING'",
                    new MapSqlParameterSource("memberId", memberId).addValue("spjangcd", spjangcd));
            if (pRow != null && ((Number) pRow.get("cnt")).longValue() > 0) {
                skipped.add(Map.of(
                        "member_id", memberId,
                        "member_name", str(member.get("member_name")),
                        "reason", "이미 진행 중인 신청 있음"));
                continue;
            }

            Map<String, Object> regRow = sqlRunner.getRow(/* skip_tenant_check */
                    """
                    INSERT INTO cms_account_register (
                        spjangcd, member_id, member_name, member_no,
                        bank_code, bank_account, id_number, member_type,
                        apply_type, apply_date, ei13_status, eb13_status, status,
                        _creater_id, _created, _modifier_id, _modified
                    ) VALUES (
                        :spjangcd, :memberId, :memberName, :memberNo,
                        :bankCode, :bankAccount, :idNumber, :memberType,
                        '3', TO_CHAR(NOW(),'YYYYMMDD'), 'SENT', 'PENDING', 'PENDING',
                        :userId, NOW(), :userId, NOW()
                    ) RETURNING id
                    """,
                    new MapSqlParameterSource()
                            .addValue("spjangcd",    spjangcd)
                            .addValue("memberId",    memberId)
                            .addValue("memberName",  str(member.get("member_name")))
                            .addValue("memberNo",    str(member.get("member_no")))
                            .addValue("bankCode",    str(member.get("bank_code")))
                            .addValue("bankAccount", str(member.get("bank_account")))
                            .addValue("idNumber",    str(member.get("id_number")))
                            .addValue("memberType",  str(member.get("member_type")))
                            .addValue("userId",      userId));

            long registerId = ((Number) regRow.get("id")).longValue();
            okMemberIds.add(memberId);
            okRegisterIds.add(registerId);
        }

        if (okRegisterIds.isEmpty()) {
            return Map.of("sent", 0, "failed", 0, "skipped", skipped,
                    "message", "해지 가능한 대상이 없습니다.");
        }

        // 해지행은 PENDING 상태로만 생성. 실제 EB13 전송은 '출금이체 인증 관리'의
        // "인증 등록 신청"에서 신규 건과 함께 하루 1파일로 묶어 보낸다(EB13 1일 1파일 제약).
        // 회원 상태는 해지 신청 접수 표시로 PENDING_CANCEL 처리.
        sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_member SET status='PENDING_CANCEL',
                    _modifier_id=:userId, _modified=NOW()
                WHERE id IN (:memberIds) AND spjangcd=:spjangcd
                """,
                new MapSqlParameterSource("memberIds", okMemberIds)
                        .addValue("spjangcd", spjangcd)
                        .addValue("userId", userId));

        return Map.of(
                "sent", okMemberIds.size(),
                "failed", 0,
                "skipped", skipped,
                "message", okMemberIds.size() + "건 해지 신청이 접수되었습니다. "
                        + "'출금이체 인증 관리'에서 인증 등록 신청으로 전송하세요."
                        + (skipped.isEmpty() ? "" : " (제외 " + skipped.size() + "건)"));
    }

    public void manualAgree(Long memberId, String userId) {
        String spjangcd = TenantContext.get();
        sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_member SET
                    agree_yn     = 'Y',
                    agree_date   = COALESCE(agree_date, CAST(NOW() AS DATE)),
                    agree_method = 'MANUAL',
                    _modifier_id = :userId,
                    _modified    = NOW()
                WHERE id = :memberId AND spjangcd = :spjangcd
                """,
                new MapSqlParameterSource()
                        .addValue("memberId", memberId)
                        .addValue("spjangcd", spjangcd)
                        .addValue("userId", userId));
    }

    /**
     * 계좌변경 신청.
     * 구계좌(cms_member 현재 계좌)로 해지행('3'), 신계좌(입력값)로 신규행('1')을
     * 같은 납부자번호로 생성한다. 두 행 모두 change_flag='Y'로 표시하여
     * EB13 전송 시 한 파일에 함께 담기고, 신규행은 EI13을 스킵하고 바로 EB13 대상이 된다.
     * cms_member의 계좌는 여기서 바로 바꾸지 않고, 신규행이 EB14 정상 승인된 뒤 갱신한다.
     * 동의상태는 미신청으로 되돌려 재인증 흐름을 태운다.
     */
    public Map<String, Object> changeAccount(Long memberId, String newBankCode,
                                             String newBankAccount, String newAccountHolder,
                                             String userId) {
        String spjangcd = TenantContext.get();

        Map<String, Object> member = sqlRunner.getRow(/* skip_tenant_check */
                """
                SELECT id, member_no, bank_code, bank_account, account_holder,
                       id_number, member_type, member_name, status
                FROM cms_member
                WHERE id = :memberId AND spjangcd = :spjangcd
                """,
                new MapSqlParameterSource("memberId", memberId).addValue("spjangcd", spjangcd));

        if (member == null) {
            return Map.of("success", false, "message", "회원을 찾을 수 없습니다.");
        }
        if (!"ACTIVE".equals(str(member.get("status")))) {
            return Map.of("success", false,
                    "message", "활성(ACTIVE) 회원만 계좌변경이 가능합니다. 현재: " + str(member.get("status")));
        }
        if (!StringUtils.hasText(newBankCode) || !StringUtils.hasText(newBankAccount)) {
            return Map.of("success", false, "message", "새 계좌 정보(은행/계좌번호)를 입력하세요.");
        }

        String memberNo   = str(member.get("member_no"));
        String oldBankCode    = str(member.get("bank_code"));
        String oldBankAccount = str(member.get("bank_account"));
        String idNumber   = str(member.get("id_number"));
        String memberType = str(member.get("member_type"));
        String memberName = str(member.get("member_name"));

        // 구계좌가 없으면 계좌변경이 아니라 신규 등록 대상 → 막는다.
        if (!StringUtils.hasText(oldBankAccount)) {
            return Map.of("success", false,
                    "message", "기존 계좌가 없어 계좌변경 대상이 아닙니다. 신규 등록을 이용하세요.");
        }

        // ★ 중복 신청 방지: 이 회원에 아직 완결되지 않은(PENDING) 등록/해지 신청이 있으면 막는다.
        //   (계좌변경을 두 번 누르거나, 해지 후 변경을 눌러 중복·모순 신청이 생기는 것을 차단)
        Map<String, Object> pendingRow = sqlRunner.getRow(/* skip_tenant_check */
                """
                SELECT COUNT(*) AS cnt
                FROM cms_account_register
                WHERE spjangcd = :spjangcd
                  AND member_id = :memberId
                  AND status = 'PENDING'
                  AND COALESCE(status,'') <> 'CANCELLED'
                """,
                new MapSqlParameterSource("memberId", memberId).addValue("spjangcd", spjangcd));
        long pendingCnt = pendingRow != null ? ((Number) pendingRow.get("cnt")).longValue() : 0;
        if (pendingCnt > 0) {
            return Map.of("success", false,
                    "message", "이미 진행 중인 등록/해지 신청이 있습니다. "
                            + "'출금이체 인증 관리'에서 기존 신청을 먼저 처리(전송/취소)한 뒤 다시 시도하세요.");
        }

        // 1) 해지행('3') - 구계좌
        //    ★ RETURNING id 로 받아 pair_id 세트키로 쓴다. 같은 회원이 계좌변경을 두 번 이상 하면
        //      member_id + change_flag 만으로는 어느 해지가 어느 신규의 짝인지 구분할 수 없다.
        Map<String, Object> cancelRow = sqlRunner.getRow(/* skip_tenant_check */
                """
                INSERT INTO cms_account_register (
                    spjangcd, member_id, member_name, member_no,
                    bank_code, bank_account, id_number, member_type,
                    apply_type, change_flag, apply_date, ei13_status, eb13_status, status,
                    _creater_id, _created, _modifier_id, _modified
                ) VALUES (
                    :spjangcd, :memberId, :memberName, :memberNo,
                    :bankCode, :bankAccount, :idNumber, :memberType,
                    '3', 'Y', TO_CHAR(NOW(),'YYYYMMDD'), 'SENT', 'PENDING', 'PENDING',
                    :userId, NOW(), :userId, NOW()
                ) RETURNING id
                """,
                new MapSqlParameterSource()
                        .addValue("spjangcd",    spjangcd)
                        .addValue("memberId",    memberId)
                        .addValue("memberName",  memberName)
                        .addValue("memberNo",    memberNo)
                        .addValue("bankCode",    oldBankCode)
                        .addValue("bankAccount", oldBankAccount)
                        .addValue("idNumber",    idNumber)
                        .addValue("memberType",  memberType)
                        .addValue("userId",      userId));

        Long pairId = cancelRow != null ? ((Number) cancelRow.get("id")).longValue() : null;
        if (pairId != null) {
            // 해지행 자신도 같은 세트키를 갖도록 채운다(세트 조회를 pair_id 하나로 통일).
            sqlRunner.execute(/* skip_tenant_check */
                    "UPDATE cms_account_register SET pair_id = :pairId WHERE id = :pairId",
                    new MapSqlParameterSource("pairId", pairId));
        }

        // ★ 신규행은 새 납부자번호로 채번한다.
        //   납부자번호는 은행코드 3 + 식별번호 뒤5 + 순번 2 구조라 은행이 바뀌면 앞자리도 바뀐다.
        //   같은 번호를 재사용하면 해지 확정 후 재신청이 되어 A016(이중신청)으로 거절된다.
        //   해지행은 은행 원장 조회 기준이므로 반드시 기존 번호(memberNo)를 그대로 유지할 것.
        String newMemberNo = generateMemberNo(spjangcd, newBankCode, idNumber);

        // 2) 신규행('1') - 신계좌. EI13(동의자료)을 타야 하므로 ei13_status=PENDING.
        //    동의서(agree_file_path)는 이후 '출금이체 인증 관리'에서 첨부 → EI13 전송.
        sqlRunner.execute(/* skip_tenant_check */
                """
                INSERT INTO cms_account_register (
                    spjangcd, member_id, member_name, member_no,
                    bank_code, bank_account, account_holder, id_number, member_type,
                    apply_type, change_flag, agree_type, apply_date, ei13_status, eb13_status, status,
                    pair_id,
                    _creater_id, _created, _modifier_id, _modified
                ) VALUES (
                    :spjangcd, :memberId, :memberName, :memberNo,
                    :bankCode, :bankAccount, :accountHolder, :idNumber, :memberType,
                    '1', 'Y', '1', TO_CHAR(NOW(),'YYYYMMDD'), 'PENDING', 'PENDING', 'PENDING',
                    :pairId,
                    :userId, NOW(), :userId, NOW()
                )
                """,
                new MapSqlParameterSource()
                        .addValue("spjangcd",      spjangcd)
                        .addValue("memberId",      memberId)
                        .addValue("memberName",    memberName)
                        .addValue("memberNo",      newMemberNo)   // ★ 신규행 = 새 번호
                        .addValue("bankCode",      newBankCode)
                        .addValue("bankAccount",   newBankAccount)
                        .addValue("accountHolder", StringUtils.hasText(newAccountHolder) ? newAccountHolder : memberName)
                        .addValue("idNumber",      idNumber)
                        .addValue("memberType",    memberType)
                        .addValue("pairId",        pairId)      // ★ 해지행 id = 세트키
                        .addValue("userId",        userId));

        // 3) 회원 정보: 계좌를 새 값으로 즉시 반영 + 동의상태는 미신청('N')으로 내림. (2026-07-15 변경)
        //
        //    ※ 이전 설계는 '승인 후 반영'이었으나, 담당자가 변경 신청한 계좌를 목록에서 바로 확인해야 해서
        //      즉시 반영으로 전환. agree_yn='N'(인증대기)이라 아직 출금이체 등록이 안 된 상태임은 유지된다.
        //
        //    ★ 전제: 청구(EB21) 생성이 agree_yn='Y' 인 회원만 대상으로 해야 한다.
        //      인증대기 회원까지 청구를 만들면, 아직 은행에 등록되지 않은 새 계좌로 출금 요청이 나가
        //      0017(미신청계좌)로 전멸한다. 청구 생성 로직에 해당 필터가 있는지 반드시 확인할 것.
        //
        //    ※ 구계좌는 cms_account_register 의 해지행(change_flag='Y', apply_type='3').bank_account 에
        //      보존된다. 신청 취소 시 그 값으로 원복한다. (CmsAccountRegisterService.cancelPending)
        sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_member SET
                    member_no      = :newMemberNo,
                    bank_code      = :bankCode,
                    bank_account   = :bankAccount,
                    account_holder = COALESCE(:accountHolder, account_holder),
                    agree_yn       = 'N',
                    agree_method   = NULL,
                    _modifier_id   = :userId,
                    _modified      = NOW()
                WHERE id = :memberId AND spjangcd = :spjangcd
                """,
                new MapSqlParameterSource("memberId", memberId)
                        .addValue("spjangcd", spjangcd)
                        .addValue("userId", userId)
                        .addValue("newMemberNo",   newMemberNo)
                        .addValue("bankCode",      newBankCode)
                        .addValue("bankAccount",   newBankAccount)
                        .addValue("accountHolder", StringUtils.hasText(newAccountHolder) ? newAccountHolder : null));

        return Map.of("success", true,
                "message", "계좌변경 신청이 접수되었습니다. '출금이체 인증 관리'에서 인증 등록 신청으로 전송하세요.");
    }

    // ── 실시간 계좌조회 (금결원 예금주명 확인) ────────────────
    // ※ 이 조회는 예금주명 확인일 뿐, 출금이체 등록(신청) 여부를 보장하지 않는다.
    @Autowired
    private CmsTokenService cmsTokenService;

    public Map<String, Object> inquiryAccount(String spjangcd,
                                              String bankCode,
                                              String accountNo,
                                              String identificationNo,
                                              String inputHolder) throws Exception {
        Map<String, Object> out = new java.util.HashMap<>();

        if (!StringUtils.hasText(bankCode) || !StringUtils.hasText(accountNo)
                || !StringUtils.hasText(identificationNo)) {
            out.put("success", false);
            out.put("responseMessage", "은행/계좌번호/식별번호를 모두 입력하세요.");
            return out;
        }

        String cleanAccount = accountNo.replaceAll("[^0-9]", "");
        String cleanIdNo    = identificationNo.replaceAll("[^0-9]", "");


        long trackingNo = Long.parseLong(
                java.time.LocalDate.now().format(java.time.format.DateTimeFormatter.BASIC_ISO_DATE)
                        + String.format("%04d", TRACKING_SEQ.updateAndGet(i -> (i + 1) % 10000)));

        com.fasterxml.jackson.databind.JsonNode data =
                cmsTokenService.realtimeAccountInquiry(
                        spjangcd, bankCode, cleanAccount, cleanIdNo, trackingNo);

        String resultCode = data.path("response_code").asText("");
        String resultMsg  = data.path("response_message").asText("");
        String depositor  = data.path("account_depositor_name").asText("");
        String txStatus   = data.path("realtime_transaction_status").asText("");

        boolean ok = "0000".equals(resultCode) && StringUtils.hasText(depositor);

        boolean holderMatched = false;
        if (ok && StringUtils.hasText(inputHolder)) {
            holderMatched = normalizeName(inputHolder).equals(normalizeName(depositor));
        }

        out.put("success", ok);
        out.put("responseCode", resultCode);
        out.put("responseMessage", resultMsg);
        out.put("depositorName", depositor);
        out.put("bankCode", bankCode);
        out.put("accountNo", cleanAccount);
        out.put("identificationNo", cleanIdNo);
        out.put("transactionStatus", txStatus);
        out.put("instituteTrackingNo", String.valueOf(trackingNo));
        out.put("holderMatched", holderMatched);
        out.put("inputHolder", inputHolder != null ? inputHolder : "");

        // ★ 조회 결과를 저장해 같은 계좌 재조회를 막는다.
        //   계좌조회는 건당 유료(결제원 중계 + 금융회사 수수료)라 반복 호출이 그대로 비용이다.
        if (ok) {
            sqlRunner.execute(/* skip_tenant_check */
                    """
                    UPDATE cms_member
                    SET inquiry_holder_name = :holder,
                        inquiry_account     = :account,
                        inquiry_at          = NOW(),
                        _modified           = NOW()
                    WHERE spjangcd = :spjangcd
                      AND bank_code = :bankCode
                      AND REPLACE(COALESCE(bank_account,''), '-', '') = :account
                    """,
                    new MapSqlParameterSource("spjangcd", spjangcd)
                            .addValue("bankCode", bankCode)
                            .addValue("account", cleanAccount)
                            .addValue("holder", depositor));
        }
        return out;
    }

    /**
     * 저장된 계좌조회 결과 — 같은 은행/계좌로 이미 조회한 이력이 있으면 돌려준다.
     * 화면은 이 값을 먼저 보여주고, 담당자가 '다시 조회'를 택할 때만 유료 API 를 호출한다.
     */
    public Map<String, Object> getInquiryCache(String spjangcd, String bankCode, String accountNo) {
        String cleanAccount = accountNo != null ? accountNo.replaceAll("[^0-9]", "") : "";
        return sqlRunner.getRow(/* skip_tenant_check */
                """
                SELECT inquiry_holder_name, inquiry_account,
                       TO_CHAR(inquiry_at, 'YYYY-MM-DD HH24:MI') AS inquiry_at
                FROM cms_member
                WHERE spjangcd = :spjangcd
                  AND bank_code = :bankCode
                  AND REPLACE(COALESCE(bank_account,''), '-', '') = :account
                  AND inquiry_holder_name IS NOT NULL
                  AND inquiry_account = :account
                ORDER BY inquiry_at DESC
                LIMIT 1
                """,
                new MapSqlParameterSource("spjangcd", spjangcd)
                        .addValue("bankCode", bankCode)
                        .addValue("account", cleanAccount));
    }

    private String normalizeName(String s) {
        return s == null ? "" : s.replaceAll("\\s+", "").trim();
    }

}