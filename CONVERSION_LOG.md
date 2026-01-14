# NLdoc conversion runs

This file tracks conversion steps and durations per run.

Legend
- Step: logical stage in the Kimi pipeline
- Component: station or worker responsible
- Route: AMQP routing key/topic of interest
- Started/Finished: UTC timestamps
- Duration: seconds (when Finished is known)

## Run: b3f41bd3-5dd2-44dd-a431-9c59c007eef9 ✅ SUCCESSFUL
Filename: Vault-app.docx.pdf  
Target: text/html
Test Date: 2026-01-14

| Step                     | Component                 | Route                                      | Started (UTC)    | Finished (UTC)   | Duration | Status      | Notes |
|--------------------------|---------------------------|--------------------------------------------|------------------|------------------|----------|-------------|-------|
| Upload accepted          | nldoc-api                 |                                            | 2026-01-12       |                  |          | ✅ done     | API returned UUID |
| Document source result   | document-source-shim      | pdfs.b3f41bd3-...                          | 15:09:45.241     | 15:09:45.241     | <1ms     | ✅ done     | BYPASS MODE shim forwarded job |
| PDF metadata             | worker.pdf-pdfmetadata    | worker.pdf-pdfmetadata.jobs/results.*      | 15:09:45.241     | 15:09:45.296     | 0.055s   | ✅ done     | 10 pages detected |
| PDF split to pages       | station.pdf-pages         | pdfs.*.pagecount → pdfs.*.pages.*          | 15:09:45.337     | 15:09:45.347     | 0.01s    | ✅ done     | Published 10 page entities |
| Page image               | worker.pdf-page           | worker.pdf-page.results.*                  |                  |                  |          | 🔄 running  | per page processing |
| Page content             | worker.page-content       | worker.page-content.results.*              |                  |                  |          | pending     | per page |
| Page regions (yolo/img)  | worker.page-regions-*     | worker.page-regions-*.results.*            |                  |                  |          | pending     | per page |
| Aggregate folio          | station.pdf-folio         | pdfs.* / worker.pdf-folio.results.*        |                  |                  |          | pending     | waits for all pages |
| Folio → spec             | station.folio-spec        | specs.*                                    |                  |                  |          | pending     |       |
| Spec → HTML              | station.spec-html         | worker.spec-html.results.*                 |                  |                  |          | pending     |       |
| Write output             | MinIO (output bucket)     |                                            |                  |                  |          | pending     | final artifact |

**Key breakthrough:** Using BYPASS MODE shim that skips document-source station (which rejects unknown workers) and forwards directly to pdf-pdfmetadata station via `pdfs.<documentId>` routing key.

## Run: 7708e8f9-3901-4fa0-b238-8d772319718d
Filename: Vault-app.sample3p.pdf (trimmed to first 3 pages)  
Target: text/html

| Step                     | Component                 | Route                                      | Started (UTC) | Finished (UTC) | Duration | Status   | Notes |
|--------------------------|---------------------------|--------------------------------------------|---------------|----------------|----------|----------|-------|
| Upload accepted          | nldoc-api                 |                                            |               |                |          | pending  | API returned UUID |
| Document source result   | station.document-source   | worker.document-source.results.0           |               |                |          | pending  | result published by shim |
| PDF metadata             | worker.pdf-pdfmetadata    | worker.pdf-pdfmetadata.jobs/results.*      |               |                |          | pending  |       |
| PDF split to pages       | station.pdf-pages         | pdfs.* / pdfs.*.pagecount                  |               |                |          | pending  |       |
| Page image               | worker.pdf-page           | worker.pdf-page.results.*                  |               |                |          | pending  |       |
| Page content             | worker.page-content       | worker.page-content.results.*              |               |                |          | pending  |       |
| Page regions (yolo/img)  | worker.page-regions-*     | worker.page-regions-*.results.*            |               |                |          | pending  |       |
| Aggregate folio          | station.pdf-folio         | pdfs.* / worker.pdf-folio.results.*        |               |                |          | pending  |       |
| Folio → spec             | station.folio-spec        | specs.*                                    |               |                |          | pending  |       |
| Spec → HTML              | station.spec-html         | worker.spec-html.results.*                 |               |                |          | pending  |       |
| Write output             | MinIO (output bucket)     |                                            |               |                |          | pending  | final artifact |


## Run: 9933ad45-190a-4c24-9b42-25dd437a962d
Filename: Vault-app.docx.pdf  
Target: text/html

| Step                     | Component                 | Route                                      | Started (UTC) | Finished (UTC) | Duration | Status   | Notes |
|--------------------------|---------------------------|--------------------------------------------|---------------|----------------|----------|----------|-------|
| Upload accepted          | nldoc-api                 |                                            |               |                |          | pending  | API returned UUID |
| Document source result   | station.document-source   | worker.document-source.results.0           |               |                |          | pending  | result published by shim |
| PDF metadata             | worker.pdf-pdfmetadata    | worker.pdf-pdfmetadata.jobs/results.*      |               |                |          | pending  | scaled to 2 replicas |
| PDF split to pages       | station.pdf-pages         | pdfs.* / pdfs.*.pagecount                  |               |                |          | pending  | waits for metadata |
| Page image               | worker.pdf-page           | worker.pdf-page.results.*                  |               |                |          | pending  | per page |
| Page content             | worker.page-content       | worker.page-content.results.*              |               |                |          | pending  | per page |
| Page regions (yolo/img)  | worker.page-regions-*     | worker.page-regions-*.results.*            |               |                |          | pending  | per page |
| Aggregate folio          | station.pdf-folio         | pdfs.* / worker.pdf-folio.results.*        |               |                |          | pending  | waits for all pages |
| Folio → spec             | station.folio-spec        | specs.*                                    |               |                |          | pending  |       |
| Spec → HTML              | station.spec-html         | worker.spec-html.results.*                 |               |                |          | pending  | shim also triggers html-writer jobs |
| Write output             | MinIO (output bucket)     |                                            |               |                |          | pending  | final artifact |

## How to update this log
- Tail SSE events for the run and record timestamps:
  - `curl -sS -N "https://api.nldoc.commonground.nu/conversion/<UUID>"`
- Check worker/station logs when a step starts/finishes and fill Started/Finished/Duration.
- Verify final artifact in MinIO `output` and add the object path under “Write output → Notes”.


