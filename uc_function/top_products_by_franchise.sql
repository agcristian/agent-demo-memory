-- =====================================================================================
-- top_products_by_franchise — UC function (OPCIONAL: "tool de datos en vivo" del agente)
-- =====================================================================================
-- Es la 3ª familia de tools del agente (DATOS): consulta el dataset de muestra público
-- `samples.bakehouse` y devuelve los productos más vendidos por franquicia/país. Es
-- OPCIONAL — el núcleo del demo es KA + memoria. Si la usas, créala en TU catálogo.esquema.
--
-- Los COMMENT (de la función y de CADA parámetro) NO son decorativos: el LLM los lee para
-- decidir cuándo llamar la tool y qué valores pasar. Por eso el de `filter_country` lista
-- los valores reales de la data (códigos como 'US', no 'Estados Unidos').
--
-- REEMPLAZA <CATALOG> y <SCHEMA>. (`samples.bakehouse` es un dataset público de Databricks,
-- presente en todo workspace con Unity Catalog; ese nombre NO se reemplaza.)
--
-- Permisos: quien ejecute la función necesita USE CATALOG/USE SCHEMA + EXECUTE. Si la usa la
-- Databricks App, otórgaselos a su service principal (ver app/README.md).
-- =====================================================================================
CREATE OR REPLACE FUNCTION <CATALOG>.<SCHEMA>.top_products_by_franchise(
    filter_franchise STRING DEFAULT NULL COMMENT 'Nombre (parcial) de la franquicia, p.ej. Golden Crumbs',
    filter_country  STRING DEFAULT NULL COMMENT 'País tal como aparece en la data (códigos): US, Japan, Australia, Canada, France, Germany, Italy, Netherlands, Sweden'
)
RETURNS TABLE (franchise_name STRING, product STRING, total_quantity BIGINT, total_revenue DOUBLE, num_transactions BIGINT)
COMMENT 'Encuentra los productos más vendidos por franquicia o país de Bakehouse, ordenados por ingresos. Filtra por filter_franchise o filter_country.'
RETURN
SELECT f.name AS franchise_name, t.product,
       SUM(CAST(t.quantity AS BIGINT))   AS total_quantity,
       SUM(CAST(t.totalPrice AS DOUBLE)) AS total_revenue,
       COUNT(*)                          AS num_transactions
FROM samples.bakehouse.sales_transactions t
JOIN samples.bakehouse.sales_franchises f ON t.franchiseID = f.franchiseID
WHERE (filter_franchise IS NULL OR LOWER(f.name)    LIKE CONCAT('%', LOWER(filter_franchise), '%'))
  AND (filter_country  IS NULL OR LOWER(f.country) LIKE CONCAT('%', LOWER(filter_country),  '%'))
GROUP BY f.name, t.product
ORDER BY total_revenue DESC;
