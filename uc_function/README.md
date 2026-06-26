# UC Function — `top_products_by_franchise` (tool de datos, opcional)

La **3ª familia de tools** del agente: **datos en vivo**. Una Unity Catalog function que
consulta el dataset público `samples.bakehouse` y devuelve los productos más vendidos por
franquicia o país, ordenados por ingresos.

Es **opcional**: el núcleo del demo es **KA + memoria**. Inclúyela si quieres mostrar que un
agente combina conocimiento (KA), memoria (Lakebase) y **datos estructurados** (SQL).

## Crearla

Reemplaza `<CATALOG>` y `<SCHEMA>` y ejecuta `top_products_by_franchise.sql`, por ejemplo:

```bash
databricks api post /api/2.0/sql/statements -p <CLI_PROFILE> --json '{
  "warehouse_id": "<SQL_WAREHOUSE_ID>",
  "statement": "'"$(sed 's/"/\\"/g' top_products_by_franchise.sql | tr '\n' ' ')"'",
  "wait_timeout": "30s"
}'
```

…o simplemente pega el SQL (ya con tu catálogo/esquema) en un editor SQL de Databricks, o
deja que lo cree el notebook (`notebook/agent_memory_demo.py`, sección 3a, con
`INCLUDE_DATA_TOOL = True`).

## Por qué importan los `COMMENT`

El LLM lee el `COMMENT` de la función y de cada parámetro para decidir **cuándo** llamarla y
**qué** valores pasar. Ojo con `filter_country`: la data usa **códigos** (`US`, `Japan`,
`Australia`, …), no nombres completos — por eso el comment los enumera. Sin ese hint, el LLM
podría pasar "Estados Unidos" y obtener 0 filas.

## Usarla desde la App

Si la consume la Databricks App, su **service principal** necesita `USE CATALOG`, `USE SCHEMA`
y `EXECUTE` sobre la función, y hay que declarar `UC_FUNCTIONS` en `app/app.yaml`
(ver ese archivo y `app/README.md`).
