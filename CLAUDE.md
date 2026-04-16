# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run Commands

```bash
# Build (skip tests)
mvn clean package -DskipTests

# Run locally (Spring Boot)
mvn spring-boot:run

# Run single test class
mvn test -Dtest=ClassName

# Run single test method
mvn test -Dtest=ClassName#methodName
```

## Architecture Overview

### Technology Stack
- **Spring Boot 2.7.18** / Java 17, packaged as WAR
- **Database**: PostgreSQL with dual data access — Spring Data JPA (entities) + raw SQL via `SqlRunner` (most queries)
- **MyBatis**: Used in select areas alongside JPA
- **View Layer**: Thymeleaf templates + static HTML/JS served at `/mes` context path
- **Cache/Session**: Redis (Spring Data Redis) + Spring Session JDBC
- **External**: Popbill (tax invoice), OpenAI API (SQL analysis), MQTT (HMI/IoT data)

### Multi-Tenancy (Critical)
This is a **multi-tenant SaaS MES**. Every tenant is identified by `spjangcd` (사업장 코드).

- `SpjangSecurityInterceptor` validates that the `spjangcd` request parameter matches the session's `spjangcd` for all `/api/**` routes — tampering is blocked with HTTP 403.
- `TenantContext` stores the current tenant's `spjangcd` in a `ThreadLocal`, recovered from HTTP session if missing.
- `SqlRunQueryImpl.checkTenantSafety()` warns (logs) when a query lacks `spjangcd` filtering. **All new SQL queries for tenant-specific data must include `AND spjangcd = :spjangcd`**.
- The comment `/* skip_tenant_check */` suppresses the warning for intentionally cross-tenant queries.

### GUI/Menu System
Pages are accessed via `/gui/{gui_code}` or `/gui/{gui_code}/{template}`. Menu codes and their Thymeleaf template paths are defined in `src/main/resources/gui.json`. `GUIConfiguration` loads this JSON at startup into a static map.

`GuiController` handles all GUI requests:
1. Looks up the template path from `gui.json`
2. Checks `user_group_menu` table for R/W permissions (`AuthCode` contains "R" and/or "W")
3. Passes `read_flag` and `write_flag` to the view
4. Logs menu access to `MenuUseLog`

**To add a new menu page**: add an entry to `gui.json` and create the corresponding Thymeleaf template.

### SQL Query Pattern
Almost all data access uses `SqlRunner` (not JPA repositories) with `NamedParameterJdbcTemplate`:

```java
@Autowired
SqlRunner sqlRunner;

MapSqlParameterSource dicParam = new MapSqlParameterSource();
dicParam.addValue("spjangcd", spjangcd);
String sql = "SELECT ... FROM table WHERE spjangcd = :spjangcd";
List<Map<String, Object>> rows = sqlRunner.getRows(sql, dicParam);
Map<String, Object> row = sqlRunner.getRow(sql, dicParam);
int affected = sqlRunner.execute(sql, dicParam);
```

JPA entities/repositories exist primarily for INSERT/UPDATE on master data. Most SELECT queries are raw SQL through `SqlRunner`.

### Package Structure
```
mes/
  app/           # Controllers + Services, organized by domain
    common/      # TenantContext, shared controllers
    interceptor/ # SpjangSecurityInterceptor, TenantSqlInspector
    filter/      # ControllerExecutionTimeAspect (AOP logging)
    system/      # User, menu, system management
    definition/  # Master data (material, BOM, equipment, etc.)
    production/  # Production results and jobs
    inventory/   # Stock management
    shipment/    # Outbound logistics
    sales/       # Orders (수주)
    balju/       # Purchase orders (발주)
    analysis/    # AI-powered SQL analysis (OpenAI)
    PopBill/     # Tax invoice / bank sync integration
    Scheduler/   # Batch jobs
  config/        # SecurityConfiguration, WebMvcConfig, Settings
  domain/
    entity/      # JPA entities
    repository/  # Spring Data JPA repositories
    security/    # Custom auth (PBKDF2-SHA256 password hashing)
    services/    # SqlRunner interface + LogWriter
  Encryption/    # AES encryption utilities
```

### Security
- **Authentication**: Custom `CustomAuthenticationManager` / `CustomAuthenticationProvider` using PBKDF2-SHA256 (`Pbkdf2Sha256`)
- Login is AJAX-based — success/failure handlers return JSON, not redirects
- CSRF is disabled for `/login`, `/postLogin`, `/api/files/upload/**`, `/popbill/webhook`, `/pda/**`
- Session stored in both JDBC (Spring Session) and Redis

### Environment / Configuration
- Sensitive values (DB credentials, API keys, Redis config) are loaded from a `server.env` file via `dotenv-java` — **not committed to git**
- `application.properties` references env vars with `${VAR_NAME}` syntax
- `BASE_PATH` env var sets the root path for file uploads and form templates
- Context path: `/mes`

### Naming Conventions
- DB columns use `"QuotedCamelCase"` (PostgreSQL quoted identifiers inherited from Django ORM)
- `spjangcd` is the universal tenant discriminator column
- Service classes follow `{Feature}Service` pattern; some older files have inconsistent casing (e.g., `bank_codeService`)
