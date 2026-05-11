package mes.app.definition.service;

import lombok.extern.slf4j.Slf4j;
import mes.app.common.TenantContext;
import mes.domain.repository.Tb_xa012Repository;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class WorkPlaceService {

    @Autowired SqlRunner sqlRunner;
    @Autowired Tb_xa012Repository tbXa012Repository;

    // ── 세무서 ────────────────────────────────────────────

    public String getTaxnm(String taxcd) {
        MapSqlParameterSource param = new MapSqlParameterSource("taxcd", taxcd);
        Map<String, Object> tax = sqlRunner.getRow(
                "SELECT taxnm FROM tb_xatax WHERE taxcd = :taxcd", param);
        return tax != null ? tax.get("taxnm").toString() : "";
    }

    public List<Map<String, Object>> getPopupList(String taxcd, String taxnm, String taxjiyuk) {
        MapSqlParameterSource param = new MapSqlParameterSource();
        StringBuilder sql = new StringBuilder("SELECT * FROM tb_xatax WHERE 1=1");
        if (taxcd != null && !taxcd.isEmpty()) {
            param.addValue("taxcd", "%" + taxcd + "%");
            sql.append(" AND taxcd LIKE :taxcd");
        }
        if (taxnm != null && !taxnm.isEmpty()) {
            param.addValue("taxnm", "%" + taxnm + "%");
            sql.append(" AND taxnm LIKE :taxnm");
        }
        if (taxjiyuk != null && !taxjiyuk.isEmpty()) {
            param.addValue("taxjiyuk", "%" + taxjiyuk + "%");
            sql.append(" AND taxjiyuk LIKE :taxjiyuk");
        }
        return sqlRunner.getRows(sql.toString(), param);
    }

    // ── 사업장 조회 (CMS + ERP 포함) ──────────────────────

    public Map<String, Object> getSpjangWithCmsErp(String spjangcd) {
        // 사업장 기본정보
        Map<String, Object> xa012 = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT * FROM tb_xa012 WHERE spjangcd = :spjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd));
        if (xa012 == null) return null;

        // ERP 먼저 조회 (ms_spjangcd 참조 필요)
        Map<String, Object> erpRow = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT * FROM tb_xa012_erp WHERE spjangcd = :spjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd));
        if (erpRow != null) {
            xa012.put("erp", erpRow);
        }

        // CMS - ERP 연동이면 ms_spjangcd 일치하는 것, 아니면 NULL인 것
        String msSpjangcd = erpRow != null ? str(erpRow.get("ms_spjangcd")) : null;
        Map<String, Object> cms = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT * FROM tb_xa012_cms WHERE spjangcd = :spjangcd AND ms_spjangcd IS NOT DISTINCT FROM :msSpjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd).addValue("msSpjangcd", msSpjangcd));
        xa012.put("cms", cms);

        return xa012;
    }

    // ── 사업장 저장 (CMS + ERP 포함) ──────────────────────

    @Transactional
    public void saveSpjangWithCmsErp(Map<String, Object> req) {
        String spjangcd = str(req.get("spjangcd"));

        // 1. tb_xa012 upsert
        MapSqlParameterSource p = new MapSqlParameterSource();
        p.addValue("spjangcd",    spjangcd);
        p.addValue("spjangnm",    req.get("spjangnm"));
        p.addValue("custperclsf", req.get("custperclsf"));
        p.addValue("saupnum",     req.get("saupnum"));
        p.addValue("prenm",       req.get("prenm"));
        p.addValue("compnum",     req.get("compnum"));
        p.addValue("biztype",     req.get("biztype"));
        p.addValue("item",        req.get("item"));
        p.addValue("openymd",     req.get("openymd"));
        p.addValue("eddate",      req.get("eddate"));
        p.addValue("tel1",        req.get("tel1"));
        p.addValue("fax",         req.get("fax"));
        p.addValue("zipcd",       req.get("zipcd"));
        p.addValue("adresa",      req.get("adresa"));
        p.addValue("adresb",      req.get("adresb"));

        sqlRunner.execute(/* skip_tenant_check */
                """
                INSERT INTO tb_xa012 (spjangcd, spjangnm, custperclsf, saupnum, prenm, compnum,
                    biztype, item, openymd, eddate, tel1, fax, zipcd, adresa, adresb, bill_plans_id)
                VALUES (:spjangcd, :spjangnm, :custperclsf, :saupnum, :prenm, :compnum,
                    :biztype, :item, :openymd, :eddate, :tel1, :fax, :zipcd, :adresa, :adresb, 1)
                ON CONFLICT (spjangcd) DO UPDATE SET
                    spjangnm    = EXCLUDED.spjangnm,
                    custperclsf = EXCLUDED.custperclsf,
                    saupnum     = EXCLUDED.saupnum,
                    prenm       = EXCLUDED.prenm,
                    compnum     = EXCLUDED.compnum,
                    biztype     = EXCLUDED.biztype,
                    item        = EXCLUDED.item,
                    openymd     = EXCLUDED.openymd,
                    eddate      = EXCLUDED.eddate,
                    tel1        = EXCLUDED.tel1,
                    fax         = EXCLUDED.fax,
                    zipcd       = EXCLUDED.zipcd,
                    adresa      = EXCLUDED.adresa,
                    adresb      = EXCLUDED.adresb
                """, p);

        // 2. CMS upsert (ms_spjangcd IS NULL 본점 행)
        @SuppressWarnings("unchecked")
        Map<String, Object> cms = (Map<String, Object>) req.get("cms");
        if (cms != null && !str(cms.get("cmsCode")).isEmpty()) {
            String msSpjangcd = str(cms.get("msSpjangcd"));
            saveCms(spjangcd, msSpjangcd.isEmpty() ? null : msSpjangcd, cms);
        }

        // 3. ERP upsert or delete
        @SuppressWarnings("unchecked")
        Map<String, Object> erp = (Map<String, Object>) req.get("erp");
        if (erp != null) {
            saveErp(spjangcd, erp);
        } else {
            sqlRunner.execute(/* skip_tenant_check */
                    "DELETE FROM tb_xa012_erp WHERE spjangcd = :spjangcd",
                    new MapSqlParameterSource("spjangcd", spjangcd));
        }
    }

    // ── CMS 조회 ──────────────────────────────────────────

    public Map<String, Object> getCms(String spjangcd) {
        return sqlRunner.getRow(/* skip_tenant_check */
                "SELECT * FROM tb_xa012_cms WHERE spjangcd = :spjangcd AND ms_spjangcd IS NULL",
                new MapSqlParameterSource("spjangcd", spjangcd));
    }

    // ── CMS 저장 (내부 공통) ──────────────────────────────

    private void saveCms(String spjangcd, String msSpjangcd, Map<String, Object> cms) {
        MapSqlParameterSource p = new MapSqlParameterSource();
        p.addValue("spjangcd",       spjangcd);
        p.addValue("msSpjangcd",     msSpjangcd);
        p.addValue("cmsCode",        cms.get("cmsCode"));
        p.addValue("cmsPassword",    cms.get("cmsPassword"));
        p.addValue("cmsDescription", cms.get("cmsDescription"));
        p.addValue("cmsState",       cms.get("cmsState"));
        p.addValue("cmsBankCode",    cms.get("cmsBankCode"));
        p.addValue("cmsRecvAccount", cms.get("cmsRecvAccount"));
        p.addValue("cmsBankBranch",  cms.get("cmsBankBranch"));

        sqlRunner.execute(/* skip_tenant_check */
                """
                INSERT INTO tb_xa012_cms (spjangcd, ms_spjangcd, cms_code, cms_password,
                    cms_description, cms_state, cms_bank_code, cms_recv_account, cms_bank_branch)
                VALUES (:spjangcd, :msSpjangcd, :cmsCode, :cmsPassword,
                    :cmsDescription, :cmsState, :cmsBankCode, :cmsRecvAccount, :cmsBankBranch)
                ON CONFLICT (spjangcd, ms_spjangcd) DO UPDATE SET
                    cms_code        = EXCLUDED.cms_code,
                    cms_password    = EXCLUDED.cms_password,
                    cms_description = EXCLUDED.cms_description,
                    cms_state       = EXCLUDED.cms_state,
                    cms_bank_code   = EXCLUDED.cms_bank_code,
                    cms_recv_account = EXCLUDED.cms_recv_account,
                    cms_bank_branch = EXCLUDED.cms_bank_branch
                """, p);
    }

    // ── ERP (tb_xa012_erp) ───────────────────────────────

    public void testErpConnection(Map<String, Object> req, String spjangcd) {
        String url = String.format("jdbc:sqlserver://%s:%s;databaseName=%s;encrypt=false",
                str(req.get("host")), str(req.get("port")), str(req.get("dbName")));
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                    url, str(req.get("username")), str(req.get("password")))) {
                log.info("[ERP연결테스트] 성공 spjangcd={}", spjangcd);
            }
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("MSSQL 드라이버 없음");
        } catch (Exception e) {
            log.warn("[ERP연결테스트] 실패 spjangcd={}: {}", spjangcd, e.getMessage());
            throw new IllegalStateException("MS DB 연결 실패: " + e.getMessage());
        }
    }

    private void saveErp(String spjangcd, Map<String, Object> erp) {
        // custcd 자동 조회 (MS DB에서 직접 가져옴)
        String custcd = str(erp.get("custcd"));
        if (custcd.isEmpty()) {
            try {
                String url = String.format("jdbc:sqlserver://%s:%s;databaseName=%s;encrypt=false",
                        str(erp.get("host")), str(erp.get("port")), str(erp.get("dbName")));
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                        url, str(erp.get("username")), str(erp.get("password")));
                     java.sql.PreparedStatement ps = conn.prepareStatement(
                             "SELECT TOP 1 custcd FROM TB_XCLIENT");
                     java.sql.ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) custcd = rs.getString("custcd");
                }
                log.info("[ERP저장] custcd 자동조회 성공: {}", custcd);
            } catch (Exception e) {
                log.warn("[ERP저장] custcd 자동조회 실패: {}", e.getMessage());
            }
        }

        MapSqlParameterSource p = new MapSqlParameterSource();
        p.addValue("spjangcd",   spjangcd);
        p.addValue("host",       erp.get("host"));
        p.addValue("port",       erp.get("port"));
        p.addValue("dbName",     erp.get("dbName"));
        p.addValue("custcd",     custcd.isEmpty() ? null : custcd);
        p.addValue("username",   erp.get("username"));
        p.addValue("password",   erp.get("password"));
        p.addValue("msSpjangcd", erp.get("msSpjangcd"));

        sqlRunner.execute(/* skip_tenant_check */
                """
                INSERT INTO tb_xa012_erp (spjangcd, host, port, db_name, custcd,
                    username, password, ms_spjangcd)
                VALUES (:spjangcd, :host, :port, :dbName, :custcd,
                    :username, :password, :msSpjangcd)
                ON CONFLICT (spjangcd) DO UPDATE SET
                    host        = EXCLUDED.host,
                    port        = EXCLUDED.port,
                    db_name     = EXCLUDED.db_name,
                    custcd      = EXCLUDED.custcd,
                    username    = EXCLUDED.username,
                    password    = EXCLUDED.password,
                    ms_spjangcd = EXCLUDED.ms_spjangcd
                """, p);
    }

    // ── 분점 ──────────────────────────────────────────────

    public List<Map<String, Object>> getBranches(String parentSpjangcd) {
        MapSqlParameterSource param = new MapSqlParameterSource("parentSpjangcd", parentSpjangcd);
        return sqlRunner.getRows(/* skip_tenant_check */
                """
                SELECT a.spjangcd, a.spjangnm, a.saupnum, a.prenm,
                       a.biztype, a.item, a.openymd, a.eddate,
                       a.tel1, a.fax, a.zipcd, a.adresa, a.adresb, a.custperclsf,
                       c.ms_spjangcd
                FROM tb_xa012 a
                LEFT JOIN tb_xa012_cms c
                       ON c.spjangcd = a.spjangcd AND c.ms_spjangcd IS NOT NULL
                WHERE a.parent_spjangcd = :parentSpjangcd
                ORDER BY a.spjangcd
                """, param);
    }

    @Transactional
    public void saveBranch(Map<String, Object> req) {
        String spjangcd = str(req.get("spjangcd"));
        String parentSpjangcd = str(req.get("parentSpjangcd"));
        boolean isNew = spjangcd.isEmpty();
        if (isNew) spjangcd = generateSpjangcd();

        MapSqlParameterSource p = new MapSqlParameterSource();
        p.addValue("spjangcd",       spjangcd);
        p.addValue("parentSpjangcd", parentSpjangcd);
        p.addValue("spjangnm",       req.get("spjangnm"));
        p.addValue("custperclsf",    req.get("custperclsf"));
        p.addValue("saupnum",        req.get("saupnum"));
        p.addValue("prenm",          req.get("prenm"));
        p.addValue("biztype",        req.get("biztype"));
        p.addValue("item",           req.get("item"));
        p.addValue("openymd",        req.get("openymd"));
        p.addValue("eddate",         req.get("eddate"));
        p.addValue("tel1",           req.get("tel1"));
        p.addValue("fax",            req.get("fax"));
        p.addValue("zipcd",          req.get("zipcd"));
        p.addValue("adresa",         req.get("adresa"));
        p.addValue("adresb",         req.get("adresb"));

        sqlRunner.execute(/* skip_tenant_check */
                """
                INSERT INTO tb_xa012 (spjangcd, parent_spjangcd, spjangnm, custperclsf, saupnum,
                    prenm, biztype, item, openymd, eddate, tel1, fax, zipcd, adresa, adresb, bill_plans_id)
                VALUES (:spjangcd, :parentSpjangcd, :spjangnm, :custperclsf, :saupnum,
                    :prenm, :biztype, :item, :openymd, :eddate, :tel1, :fax, :zipcd, :adresa, :adresb, 1)
                ON CONFLICT (spjangcd) DO UPDATE SET
                    spjangnm       = EXCLUDED.spjangnm,
                    custperclsf    = EXCLUDED.custperclsf,
                    saupnum        = EXCLUDED.saupnum,
                    prenm          = EXCLUDED.prenm,
                    biztype        = EXCLUDED.biztype,
                    item           = EXCLUDED.item,
                    openymd        = EXCLUDED.openymd,
                    eddate         = EXCLUDED.eddate,
                    tel1           = EXCLUDED.tel1,
                    fax            = EXCLUDED.fax,
                    zipcd          = EXCLUDED.zipcd,
                    adresa         = EXCLUDED.adresa,
                    adresb         = EXCLUDED.adresb
                """, p);

        // ERP 연동 분점인 경우 tb_xa012_cms에 ms_spjangcd 연결
        String msSpjangcd = str(req.get("msSpjangcd"));
        if (!msSpjangcd.isEmpty()) {
            saveCms(spjangcd, msSpjangcd, new java.util.HashMap<>());
        }
    }

    @Transactional
    public void deleteBranch(String spjangcd) {
        sqlRunner.execute(/* skip_tenant_check */
                "DELETE FROM tb_xa012_cms WHERE spjangcd = :spjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd));
        sqlRunner.execute(/* skip_tenant_check */
                "DELETE FROM tb_xa012 WHERE spjangcd = :spjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd));
    }

    // spjangcd 채번: 대문자 2자리 조합, MAX + 1
    private String generateSpjangcd() {
        Map<String, Object> row = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT MAX(spjangcd) AS max FROM tb_xa012 WHERE spjangcd ~ '^[A-Z]{2}$'",
                new MapSqlParameterSource());
        String max = row != null ? str(row.get("max")) : "";
        if (max.isEmpty()) return "AA";
        char c1 = max.charAt(0);
        char c2 = max.charAt(1);
        if (c2 < 'Z') return "" + c1 + (char)(c2 + 1);
        if (c1 < 'Z') return "" + (char)(c1 + 1) + 'A';
        throw new IllegalStateException("사업장코드 채번 범위 초과");
    }

    // WorkPlaceService
    public List<Map<String, Object>> getBankList() {
        return sqlRunner.getRows(/* skip_tenant_check */
                "SELECT bank_code, bank_name FROM cms_bank_code WHERE use_yn = 'Y' ORDER BY bank_code",
                new MapSqlParameterSource());
    }

    // ── 유틸 ──────────────────────────────────────────────

    private String str(Object v) { return v != null ? v.toString() : ""; }
}