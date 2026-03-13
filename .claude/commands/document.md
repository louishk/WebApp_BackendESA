# Document

Generate comprehensive documentation for a tool, API endpoint, pipeline, or module. Outputs structured YAML (or OpenAPI for APIs) into the `docs/` folder for version-controlled reference.

Usage: `/document <target>`
Examples:
- `/document tool:unit_availability` — document the unit availability tool page + its API endpoints
- `/document api:reservations` — document all endpoints in the reservations blueprint (OpenAPI)
- `/document api:all` — document all API endpoints across all route files (OpenAPI)
- `/document pipeline:rent_roll` — document the rent roll ETL pipeline
- `/document soap:GetSiteUnitData` — document a SOAP operation
- `/document module:common/soap_client.py` — document a shared module

Target: $ARGUMENTS

---

## Step 1: Identify What to Document

Parse the target argument:

| Prefix | Scope | Output Format | Output Path |
|--------|-------|---------------|-------------|
| `tool:` | Tool template + its backing API endpoints + route handlers | YAML | `docs/tools/<name>.yaml` |
| `api:` | Route blueprint or specific endpoint(s) | OpenAPI 3.0 YAML | `docs/api/<blueprint>.yaml` |
| `pipeline:` | ETL pipeline module + config + schedule | YAML | `docs/pipelines/<name>.yaml` |
| `soap:` | SOAP operation (request/response/fields) | YAML | `docs/soap/<operation>.yaml` |
| `module:` | Shared module (classes, functions, usage) | YAML | `docs/modules/<name>.yaml` |

If `api:all` is specified, generate a single consolidated OpenAPI spec at `docs/api/openapi.yaml`.

---

## Step 2: Read & Analyze Source Code

Thoroughly read ALL relevant source files. For each target type:

### Tool (`tool:`)
Read:
- Template: `backend/python/web/templates/tools/<name>.html`
- Route handler(s) in `backend/python/web/routes/tools.py` and `backend/python/web/routes/api.py`
- Any models referenced
- Any SOAP calls made

Extract:
- Tool name and purpose
- URL path and access permissions (decorators)
- UI components and user interactions (buttons, forms, filters, tables)
- API endpoints the frontend calls (from `fetch()` calls in the template JS)
- Request/response formats for each endpoint
- Data sources (which DB, which tables/views, which SOAP calls)
- Business logic and data transformations
- Error handling behavior
- Dependencies (other tools, external APIs)

### API (`api:`)
Read:
- Route file: `backend/python/web/routes/<blueprint>.py`
- Referenced models
- Auth decorators and scopes

Extract per endpoint:
- HTTP method and path
- Authentication method (JWT, session, none)
- Required scopes/permissions
- Request parameters (query, path, body) with types and constraints
- Request body schema (if POST/PUT/PATCH)
- Response schema for success and error cases
- Rate limiting rules
- Database queries performed
- SOAP calls made
- Business logic summary
- Example request/response

### Pipeline (`pipeline:`)
Read:
- Pipeline module: `backend/python/datalayer/<name>.py`
- Config: `backend/python/config/pipelines.yaml` (the relevant entry)
- Schedule: `backend/python/config/scheduler.yaml` (the relevant entry)
- Any models/tables written to

Extract:
- Pipeline name and purpose
- Schedule (cron expression, frequency)
- Data source (SOAP API, external API, DB query)
- Data destination (which DB, which table)
- Transformation steps
- Error handling and retry logic
- Row counts / volume expectations
- Dependencies (other pipelines, external services)

### SOAP (`soap:`)
Read:
- Usage in codebase (grep for the operation name)
- SOAP client: `backend/python/common/soap_client.py`
- Any wrapper functions

Extract:
- Operation name and WSDL context
- Purpose / business function
- Request parameters (name, type, required, description)
- Response fields (name, type, description)
- Authentication (auto-injected fields)
- Known quirks or data quality issues
- Where it's called from in the codebase

### Module (`module:`)
Read:
- The module file itself
- Files that import from it

Extract:
- Module purpose
- Classes and their responsibilities
- Public functions (signature, purpose, parameters, return type)
- Configuration it reads
- Dependencies
- Usage examples from the codebase

---

## Step 3: Generate Documentation

Create the output file in the appropriate `docs/` subfolder.

### For Tool docs — YAML format:

```yaml
tool:
  name: <display name>
  description: <what it does and why>
  url_path: <Flask route path>
  template: <template file path>
  access:
    decorator: <e.g., @billing_tools_access_required>
    required_role: <role or permission>

  ui:
    layout: <description of page layout>
    components:
      - type: <filter|table|button|form|modal|chart>
        name: <component name>
        description: <what it does>
        interactions: <user actions available>

  api_endpoints:
    - method: <GET|POST|PUT|DELETE>
      path: <endpoint path>
      purpose: <what this endpoint does>
      auth: <JWT|session|none>
      scope: <API scope if JWT>
      rate_limit: <requests per minute if set>
      parameters:
        - name: <param name>
          in: <query|path|body>
          type: <string|integer|boolean|array|object>
          required: <true|false>
          description: <what it controls>
          example: <example value>
      request_body:  # if applicable
        content_type: application/json
        schema:
          <field>:
            type: <type>
            description: <description>
            example: <example>
      response:
        success:
          status: <200|201>
          schema:
            status: success
            data: <describe structure>
          example: <full example response>
        error:
          status: <400|401|403|404|500>
          schema:
            error: <generic error message>

  data_sources:
    - type: <database|soap|external_api>
      name: <table/view/operation name>
      database: <esa_backend|esa_pbi>
      description: <what data comes from here>
      key_fields:
        - <field name>: <description>

  business_logic:
    - step: <step description>
      details: <how it works>

  known_issues:
    - <any quirks, limitations, or data quality notes>

  related:
    - <other tools, pipelines, or endpoints that interact with this>
```

### For API docs — OpenAPI 3.0 format:

```yaml
openapi: "3.0.3"
info:
  title: <Blueprint Name> API
  description: <Blueprint purpose>
  version: "1.0.0"

servers:
  - url: /api
    description: ESA Backend API

components:
  securitySchemes:
    bearerAuth:
      type: http
      scheme: bearer
      bearerFormat: JWT
    sessionAuth:
      type: apiKey
      in: cookie
      name: session

  schemas:
    # Define reusable schemas here
    Error:
      type: object
      properties:
        error:
          type: string

paths:
  <path>:
    <method>:
      summary: <short description>
      description: <detailed description including business logic>
      operationId: <function name>
      security:
        - bearerAuth: []
      tags:
        - <group name>
      parameters:
        - name: <param>
          in: <query|path>
          required: <true|false>
          schema:
            type: <type>
          description: <description>
          example: <example>
      requestBody:  # if applicable
        required: true
        content:
          application/json:
            schema:
              type: object
              properties:
                <field>:
                  type: <type>
                  description: <description>
            example:
              <full example>
      responses:
        "200":
          description: <success description>
          content:
            application/json:
              schema:
                type: object
                properties:
                  status:
                    type: string
                    example: success
                  data:
                    <describe structure>
              example:
                <full example response>
        "400":
          description: Bad request
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Error"
        "401":
          description: Unauthorized
        "403":
          description: Forbidden — insufficient scope
        "429":
          description: Rate limited
```

### For Pipeline docs — YAML format:

```yaml
pipeline:
  name: <pipeline name>
  description: <purpose>
  module: <file path>

  schedule:
    cron: <cron expression>
    frequency: <human-readable>
    timezone: <timezone>

  source:
    type: <soap|rest_api|database|bigquery>
    operation: <operation name or endpoint>
    parameters:
      - name: <param>
        value: <value or dynamic>
        description: <purpose>

  destination:
    database: <esa_backend|esa_pbi>
    table: <table name>
    strategy: <upsert|replace|append>
    key_columns:
      - <column name>

  transformations:
    - step: <step number>
      action: <what happens>
      details: <specifics>

  error_handling:
    retry: <retry strategy>
    on_failure: <what happens>
    alerting: <alert config if any>

  volume:
    typical_rows: <approximate>
    runtime: <typical duration>

  dependencies:
    upstream:
      - <what must run first>
    downstream:
      - <what depends on this>
```

### For SOAP docs — YAML format:

```yaml
soap_operation:
  name: <operation name>
  service: <CallCenterWs or other>
  description: <business purpose>

  authentication:
    auto_injected: true
    fields:
      - sCorpCode
      - sCorpUserName
      - sCorpPassword

  request:
    parameters:
      - name: <field name>
        type: <string|int|boolean|date>
        required: <true|false>
        description: <what it controls>
        example: <example value>
        notes: <any quirks>

  response:
    root_element: <root XML element>
    fields:
      - name: <field name>
        type: <type>
        description: <what it contains>
        example: <example value>
        reliability: <reliable|unreliable>
        notes: <any data quality issues>

  usage_in_codebase:
    - file: <file path>
      function: <function name>
      purpose: <why it's called here>

  known_issues:
    - <data quality notes, unreliable fields, site-specific quirks>

  related_operations:
    - <other SOAP operations often used together>
```

### For Module docs — YAML format:

```yaml
module:
  name: <module name>
  path: <file path>
  description: <purpose>

  classes:
    - name: <class name>
      description: <purpose>
      methods:
        - name: <method name>
          description: <what it does>
          parameters:
            - name: <param>
              type: <type>
              description: <description>
          returns:
            type: <type>
            description: <what it returns>

  functions:
    - name: <function name>
      description: <what it does>
      parameters:
        - name: <param>
          type: <type>
          required: <true|false>
          description: <description>
      returns:
        type: <type>
        description: <what it returns>
      example: <usage example from codebase>

  configuration:
    - source: <config file or env var>
      keys:
        - <key>: <what it configures>

  used_by:
    - file: <file path>
      purpose: <why it imports this module>

  dependencies:
    - <external packages or other modules>
```

---

## Step 4: Security Scrub

Before writing the file, scan the generated content for:

- API keys, tokens, passwords, secrets, salt values
- Database connection strings with credentials
- IP addresses with credentials (IP alone is OK if no auth context)
- Private keys or certificates
- Any value from `.env` or `app_secrets`

**If any sensitive data is found**: replace with `<REDACTED>` and add a comment noting what was removed.

Hostnames, ports, URL paths, table names, column names, and parameter names are fine to include.

---

## Step 5: Write & Report

1. Create the docs subfolder if it doesn't exist (`docs/tools/`, `docs/api/`, etc.)
2. Write the YAML/OpenAPI file
3. Report to the user:
   - What was documented
   - Output file path
   - Any sensitive data that was redacted
   - Any areas where information was incomplete (couldn't determine from code alone)

---

## Rules
- NEVER include secrets, API keys, passwords, tokens, or salt values in documentation
- Be thorough — include every parameter, every field, every edge case. This is reference documentation.
- Use real examples from the code, not placeholder values (except for secrets)
- Document known quirks and data quality issues — these are the hardest things to rediscover later
- If a field or behavior is ambiguous, note it as such rather than guessing
- Keep OpenAPI specs valid — they should parse without errors
- One file per target — don't combine multiple tools or blueprints into one file
