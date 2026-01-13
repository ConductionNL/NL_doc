# NLdoc conversion runs

This file tracks conversion steps and durations per run.

Legend
- Step: logical stage in the Kimi pipeline
- Component: station or worker responsible
- Route: AMQP routing key/topic of interest
- Started/Finished: UTC timestamps
- Duration: seconds (when Finished is known)

## Run: b3f41bd3-5dd2-44dd-a431-9c59c007eef9
Filename: Vault-app.docx.pdf  
Target: text/html

| Step                     | Component                 | Route                                      | Started (UTC) | Finished (UTC) | Duration | Status   | Notes |
|--------------------------|---------------------------|--------------------------------------------|---------------|----------------|----------|----------|-------|
| Upload accepted          | nldoc-api                 |                                            |               |                |          | pending  | API returned UUID |
| Document source result   | station.document-source   | worker.document-source.results.0           |               |                |          | pending  | result published by shim |
| PDF metadata             | worker.pdf-pdfmetadata    | worker.pdf-pdfmetadata.jobs/results.*      |               |                |          | pending  | current bottleneck |
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


