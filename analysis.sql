-- Deforestation-risk assessment specified in SPEC.md
-- Run from the project root with: duckdb analysis.duckdb < analysis.sql
-- DuckDB 1.4.x with the bundled spatial and httpfs extensions is required.

LOAD spatial;
LOAD httpfs;

SET threads = 4;
SET preserve_insertion_order = true;

CREATE OR REPLACE TABLE run_metadata AS
SELECT current_timestamp AS run_at_utc,
       'goias-sample.csv' AS input_file,
       'https://data.source.coop/wri-data-lab/trazofields/trazo3-fields/trazo3_brazil_goias_2024.parquet' AS trazo3_source,
       'https://data.source.coop/tristangruppwri/cadastral/brazil-car-area-imovel/brazil_car_area_imovel.parquet' AS car_source,
       'https://data.source.coop/tristangruppwri/soft-commodity-infrastructure/facilities/BR_facilities.parquet' AS facility_source,
       0.667::DOUBLE AS field_overlap_threshold,
       25.0::DOUBLE AS union_buffer_m,
       10000.0::DOUBLE AS override_distance_m;

-- A coarse Brazil extent is used only to identify obviously transposed axes. The
-- repair is applied only if the original is outside and its flipped form is inside.
CREATE OR REPLACE TABLE input_rows AS
WITH raw AS (
  SELECT row_number() OVER ()::INTEGER AS input_row,
         nullif(trim(cod_imovel), '') AS supplied_cod_imovel,
         nullif(trim(municipio), '') AS supplied_municipio,
         nullif(trim(cod_estado), '') AS supplied_cod_estado,
         geometry AS supplied_wkt,
         try(ST_GeomFromText(geometry)) AS original_geometry
  FROM read_csv('goias-sample.csv', header = true, all_varchar = true)
), checks AS (
  SELECT *,
    original_geometry IS NOT NULL
      AND ST_XMin(original_geometry) >= -74 AND ST_XMax(original_geometry) <= -34
      AND ST_YMin(original_geometry) >= -34 AND ST_YMax(original_geometry) <= 6
      AS original_in_brazil_extent,
    original_geometry IS NOT NULL
      AND ST_XMin(ST_FlipCoordinates(original_geometry)) >= -74
      AND ST_XMax(ST_FlipCoordinates(original_geometry)) <= -34
      AND ST_YMin(ST_FlipCoordinates(original_geometry)) >= -34
      AND ST_YMax(ST_FlipCoordinates(original_geometry)) <= 6
      AS flipped_in_brazil_extent
  FROM raw
)
SELECT *,
  CASE WHEN NOT original_in_brazil_extent AND flipped_in_brazil_extent
       THEN ST_FlipCoordinates(original_geometry) ELSE original_geometry END AS input_geometry,
  (NOT original_in_brazil_extent AND flipped_in_brazil_extent) AS axes_repaired,
  md5(concat_ws('|', coalesce(supplied_cod_imovel, ''),
                     coalesce(supplied_municipio, ''),
                     coalesce(supplied_cod_estado, ''),
                     coalesce(supplied_wkt, ''))) AS submission_key
FROM checks;

-- Cache only Goiás CAR plus a small window around the intentionally outlying
-- ID-less point. cod_estado is a Parquet predicate and avoids a nationwide cache.
CREATE OR REPLACE TABLE car_cache AS
SELECT cod_tema, nom_tema, cod_imovel, mod_fiscal, num_area, ind_status,
       ind_tipo, des_condic, municipio, cod_estado, dat_criaca, dat_atuali,
       geometry
FROM read_parquet('https://data.source.coop/tristangruppwri/cadastral/brazil-car-area-imovel/brazil_car_area_imovel.parquet')
WHERE cod_estado = 'GO'
   OR (bbox.xmin <= -35.0 AND bbox.xmax >= -35.0
       AND bbox.ymin <= -10.0 AND bbox.ymax >= -10.0);

-- ID rows resolve by ID. ID-less points resolve by containment. ID-less polygons
-- require a near-exact two-way area match (>=99.9% in each direction); this makes
-- "geometric match" auditable and prevents a small polygon merely inside a CAR
-- from being mistaken for the parcel itself. Ambiguities break on cod_imovel.
CREATE OR REPLACE TABLE property_resolution AS
WITH exact_id AS (
  SELECT i.input_row, c.cod_imovel, 'id_exact' AS resolution_method, 1.0 AS match_score
  FROM input_rows i JOIN car_cache c ON i.supplied_cod_imovel = c.cod_imovel
), point_matches AS (
  SELECT i.input_row, c.cod_imovel, 'point_containment' AS resolution_method,
         1.0 AS match_score
  FROM input_rows i JOIN car_cache c
    ON i.supplied_cod_imovel IS NULL
   AND ST_GeometryType(i.input_geometry) = 'POINT'
   AND ST_Covers(c.geometry, i.input_geometry)
), polygon_scores AS (
  SELECT i.input_row, c.cod_imovel,
         ST_Area(ST_Intersection(
           ST_Transform(i.input_geometry, 'EPSG:4326', 'EPSG:5880', always_xy := true),
           ST_Transform(c.geometry, 'EPSG:4326', 'EPSG:5880', always_xy := true)))
           / nullif(ST_Area(ST_Transform(i.input_geometry, 'EPSG:4326', 'EPSG:5880', always_xy := true)), 0)
           AS input_covered,
         ST_Area(ST_Intersection(
           ST_Transform(i.input_geometry, 'EPSG:4326', 'EPSG:5880', always_xy := true),
           ST_Transform(c.geometry, 'EPSG:4326', 'EPSG:5880', always_xy := true)))
           / nullif(ST_Area(ST_Transform(c.geometry, 'EPSG:4326', 'EPSG:5880', always_xy := true)), 0)
           AS car_covered
  FROM input_rows i JOIN car_cache c
    ON i.supplied_cod_imovel IS NULL
   AND ST_GeometryType(i.input_geometry) IN ('POLYGON', 'MULTIPOLYGON')
   AND ST_Intersects(i.input_geometry, c.geometry)
), polygon_matches AS (
  SELECT input_row, cod_imovel, 'polygon_geometric_match' AS resolution_method,
         least(input_covered, car_covered) AS match_score
  FROM polygon_scores WHERE input_covered >= 0.999 AND car_covered >= 0.999
), candidates AS (
  SELECT * FROM exact_id UNION ALL SELECT * FROM point_matches UNION ALL SELECT * FROM polygon_matches
), ranked AS (
  SELECT *, row_number() OVER (PARTITION BY input_row ORDER BY match_score DESC, cod_imovel) AS rn,
         count(*) OVER (PARTITION BY input_row) AS candidate_count
  FROM candidates
)
SELECT i.input_row, i.submission_key, i.supplied_cod_imovel,
       r.cod_imovel AS resolved_cod_imovel, r.resolution_method, r.match_score,
       coalesce(r.candidate_count, 0) AS resolution_candidate_count,
       CASE
         WHEN i.original_geometry IS NULL THEN 'missing_or_invalid_geometry'
         WHEN i.supplied_cod_imovel IS NOT NULL AND r.cod_imovel IS NULL THEN 'missing_car_record'
         WHEN i.supplied_cod_imovel IS NULL AND r.cod_imovel IS NULL THEN 'unresolved_geometry'
         WHEN r.candidate_count > 1 THEN 'resolved_with_tiebreak'
         ELSE 'resolved'
       END AS resolution_status,
       i.axes_repaired
FROM input_rows i LEFT JOIN ranked r ON i.input_row = r.input_row AND r.rn = 1;

CREATE OR REPLACE TABLE properties AS
WITH resolved AS (
  SELECT r.resolved_cod_imovel AS property_key, r.resolved_cod_imovel,
         min(r.resolution_method) AS resolution_method,
         min(r.resolution_status) AS resolution_status,
         bool_or(r.axes_repaired) AS axes_repaired,
         string_agg(r.input_row::VARCHAR, ', ' ORDER BY r.input_row) AS input_rows,
         count(*) AS input_row_count,
         any_value(c.municipio) AS municipio,
         any_value(c.cod_estado) AS cod_estado,
         any_value(c.geometry) AS geometry
  FROM property_resolution r JOIN car_cache c ON r.resolved_cod_imovel = c.cod_imovel
  GROUP BY r.resolved_cod_imovel
), unresolved AS (
  SELECT 'UNRESOLVED:' || r.submission_key AS property_key, NULL AS resolved_cod_imovel,
         NULL AS resolution_method, r.resolution_status, r.axes_repaired,
         string_agg(r.input_row::VARCHAR, ', ' ORDER BY r.input_row) AS input_rows,
         count(*) AS input_row_count,
         any_value(i.supplied_municipio) AS municipio,
         any_value(i.supplied_cod_estado) AS cod_estado,
         any_value(i.input_geometry) AS geometry
  FROM property_resolution r JOIN input_rows i USING (input_row)
  WHERE r.resolved_cod_imovel IS NULL
  GROUP BY r.submission_key, r.resolution_status, r.axes_repaired
)
SELECT * FROM resolved UNION ALL SELECT * FROM unresolved;

-- Trazo3 is a Goiás-only layer. Bounding-box predicates limit remote reads to the
-- portfolio envelope while retaining all fields that could touch a 25 m buffer.
CREATE OR REPLACE TABLE field_cache AS
SELECT Id, hansen_covered_area, hansen_loss_area, mode_year, firstyear,
       firstyearmajority, deforestarea0104, deforestarea0509,
       deforestarea1014, deforestarea1520, deforestarea2124, mbmode24,
       mbcov_area_2024, mbvalid_area_2024, geometry
FROM read_parquet('https://data.source.coop/wri-data-lab/trazofields/trazo3-fields/trazo3_brazil_goias_2024.parquet')
WHERE bbox.xmin <= -51.0 AND bbox.xmax >= -51.7
  AND bbox.ymin <= -15.3 AND bbox.ymax >= -16.1;

CREATE OR REPLACE TABLE projected_parcels AS
SELECT resolved_cod_imovel AS cod_imovel,
       ST_Transform(geometry, 'EPSG:4326', 'EPSG:5880', always_xy := true) AS geom_5880
FROM properties WHERE resolved_cod_imovel IS NOT NULL;

CREATE OR REPLACE TABLE projected_fields AS
SELECT Id AS field_id, mbmode24::INTEGER AS mbmode24, deforestarea2124,
       geometry,
       ST_Transform(geometry, 'EPSG:4326', 'EPSG:5880', always_xy := true) AS geom_5880
FROM field_cache;

CREATE OR REPLACE TABLE field_parcel_overlap AS
SELECT f.field_id, p.cod_imovel,
       ST_Area(f.geom_5880) AS field_area_m2,
       ST_Area(ST_Intersection(f.geom_5880, p.geom_5880)) / nullif(ST_Area(f.geom_5880), 0)
         AS parcel_overlap_fraction
FROM projected_fields f JOIN projected_parcels p ON ST_Intersects(f.geom_5880, p.geom_5880);

CREATE OR REPLACE TABLE field_match_diagnostics AS
WITH parcel_union AS (
  SELECT ST_Union_Agg(geom_5880) AS geom_5880 FROM projected_parcels
), buffered_union AS (
  SELECT ST_Buffer(geom_5880, 25.0) AS geom_5880 FROM parcel_union
), intersecting AS (
  SELECT f.field_id,
         ST_Area(ST_Intersection(f.geom_5880, u.geom_5880)) / nullif(ST_Area(f.geom_5880), 0)
           AS buffered_union_overlap_fraction
  FROM projected_fields f CROSS JOIN buffered_union u
  WHERE ST_Intersects(f.geom_5880, u.geom_5880)
), direct AS (
  SELECT field_id, max(parcel_overlap_fraction) AS max_parcel_overlap_fraction
  FROM field_parcel_overlap GROUP BY field_id
)
SELECT f.field_id, coalesce(d.max_parcel_overlap_fraction, 0) AS max_parcel_overlap_fraction,
       coalesce(i.buffered_union_overlap_fraction, 0) AS buffered_union_overlap_fraction,
       coalesce(d.max_parcel_overlap_fraction, 0) >= 0.667 AS matched_direct,
       coalesce(i.buffered_union_overlap_fraction, 0) >= 0.667 AS matched_buffered_union,
       (coalesce(d.max_parcel_overlap_fraction, 0) >= 0.667
        OR coalesce(i.buffered_union_overlap_fraction, 0) >= 0.667) AS matched
FROM projected_fields f LEFT JOIN direct d USING (field_id) LEFT JOIN intersecting i USING (field_id)
WHERE d.field_id IS NOT NULL OR i.field_id IS NOT NULL;

-- Every matched field is assigned to the parcel with the largest area fraction;
-- lowest cod_imovel breaks exact ties as required.
CREATE OR REPLACE TABLE matched_fields AS
WITH qualifying AS (
  SELECT d.field_id FROM field_match_diagnostics d WHERE d.matched
), assignments AS (
  SELECT o.*, row_number() OVER (
    PARTITION BY o.field_id ORDER BY o.parcel_overlap_fraction DESC, o.cod_imovel) AS rn
  FROM field_parcel_overlap o JOIN qualifying q USING (field_id)
), classed AS (
  SELECT a.cod_imovel, f.field_id, f.mbmode24, f.deforestarea2124,
         a.field_area_m2, a.parcel_overlap_fraction,
         CASE f.mbmode24
           WHEN 15 THEN 'Pasture' WHEN 21 THEN 'Mosaic of Uses'
           WHEN 35 THEN 'Palm Oil' WHEN 39 THEN 'Soybean' WHEN 46 THEN 'Coffee'
           WHEN 9 THEN 'Forest Plantation' WHEN 18 THEN 'Agriculture'
           WHEN 20 THEN 'Sugarcane' WHEN 40 THEN 'Rice'
           WHEN 41 THEN 'Other Temporary Crops' WHEN 47 THEN 'Citrus'
           WHEN 48 THEN 'Other Perennial' WHEN 62 THEN 'Cotton'
           ELSE 'Outside relevant commodity set' END AS class_name,
         f.mbmode24 IN (15, 21, 35, 39, 46) AS relevant_commodity,
         CASE f.mbmode24 WHEN 15 THEN 'cattle' WHEN 21 THEN 'cattle'
           WHEN 35 THEN 'oil palm' WHEN 39 THEN 'soya' WHEN 46 THEN 'coffee' END AS commodity,
         f.geometry
  FROM assignments a JOIN projected_fields f USING (field_id) WHERE a.rn = 1
)
SELECT * FROM classed;

CREATE OR REPLACE TABLE property_field_summary AS
WITH dominant AS (
  SELECT cod_imovel, mbmode24 AS dominant_mbmode24, class_name AS dominant_class,
         sum(field_area_m2) AS dominant_class_area_m2,
         row_number() OVER (PARTITION BY cod_imovel
                            ORDER BY sum(field_area_m2) DESC, mbmode24 ASC) AS rn
  FROM matched_fields GROUP BY cod_imovel, mbmode24, class_name
), totals AS (
  SELECT cod_imovel, count(*) AS matched_field_count,
         count(*) FILTER (WHERE relevant_commodity) AS relevant_field_count,
         count(*) FILTER (WHERE relevant_commodity AND deforestarea2124 > 0) AS loss_field_count,
         sum(CASE WHEN relevant_commodity THEN coalesce(deforestarea2124, 0) ELSE 0 END) / 10000.0
           AS post2020_loss_ha,
         count(*) FILTER (WHERE class_name = 'Outside relevant commodity set') AS unsupported_class_field_count
  FROM matched_fields GROUP BY cod_imovel
)
SELECT t.*, d.dominant_mbmode24, d.dominant_class, d.dominant_class_area_m2 / 10000.0 AS dominant_class_area_ha
FROM totals t JOIN dominant d USING (cod_imovel) WHERE d.rn = 1;

-- The facility file is small enough to cache and preserves both municipality
-- polygons (membership_muni) and routed facility points.
CREATE OR REPLACE TABLE facility_cache AS
SELECT entity_id, entity_kind, tier, weight, basis, geom_method, source, geometry
FROM read_parquet('https://data.source.coop/tristangruppwri/soft-commodity-infrastructure/facilities/BR_facilities.parquet');

CREATE OR REPLACE TABLE contact_candidates AS
WITH flagged AS (
  SELECT p.resolved_cod_imovel AS cod_imovel, p.geometry,
         ST_Centroid(p.geometry) AS centroid, s.dominant_mbmode24
  FROM properties p JOIN property_field_summary s ON p.resolved_cod_imovel = s.cod_imovel
  WHERE s.post2020_loss_ha > 0
), membership AS (
  SELECT f.cod_imovel, c.entity_id, c.entity_kind, c.tier, c.weight, c.basis,
         NULL::DOUBLE AS distance_m, false AS override_eligible
  FROM flagged f JOIN facility_cache c
    ON c.tier = 'membership_muni' AND c.entity_id = split_part(f.cod_imovel, '-', 2)
), routed AS (
  SELECT f.cod_imovel, c.entity_id, c.entity_kind, c.tier, c.weight, c.basis,
         ST_Distance(
           ST_Transform(f.centroid, 'EPSG:4326', 'EPSG:5880', always_xy := true),
           ST_Transform(c.geometry, 'EPSG:4326', 'EPSG:5880', always_xy := true)) AS distance_m,
         (c.tier IN ('intake_point', 'slaughter_point')) AS override_eligible
  FROM flagged f JOIN facility_cache c ON
       (CASE f.dominant_mbmode24
          WHEN 15 THEN c.tier = 'slaughter_point'
          WHEN 21 THEN c.tier = 'slaughter_point'
          WHEN 39 THEN c.tier = 'intake_point'
          WHEN 18 THEN c.tier IN ('intake_point', 'mill_point')
          WHEN 20 THEN c.tier = 'mill_point'
          WHEN 41 THEN c.tier = 'intake_point'
          ELSE false END)
), all_candidates AS (
  SELECT * FROM membership UNION ALL SELECT * FROM routed
)
SELECT * FROM all_candidates;

CREATE OR REPLACE TABLE selected_contacts AS
WITH ranked AS (
  SELECT *,
    row_number() OVER (PARTITION BY cod_imovel ORDER BY
      CASE WHEN override_eligible AND distance_m < 10000 THEN 0
           WHEN tier = 'membership_muni' THEN 1 ELSE 2 END,
      distance_m ASC NULLS LAST, weight DESC NULLS LAST, entity_id ASC) AS rn
  FROM contact_candidates
)
SELECT cod_imovel, entity_id AS contact_entity_id, entity_kind AS contact_entity_type,
       tier AS contact_tier, basis AS contact_evidence_basis, weight AS contact_evidence_value,
       distance_m AS contact_distance_m,
       (override_eligible AND distance_m < 10000) AS facility_override
FROM ranked WHERE rn = 1;

CREATE OR REPLACE TABLE property_results AS
SELECT p.property_key, p.resolved_cod_imovel AS cod_imovel, p.municipio, p.cod_estado,
       p.input_rows, p.input_row_count, p.resolution_status, p.resolution_method, p.axes_repaired,
       coalesce(s.matched_field_count, 0) AS matched_field_count,
       coalesce(s.relevant_field_count, 0) AS relevant_field_count,
       coalesce(s.loss_field_count, 0) AS loss_field_count,
       s.dominant_mbmode24, s.dominant_class,
       round(s.dominant_class_area_ha, 3) AS dominant_class_area_ha,
       round(coalesce(s.post2020_loss_ha, 0), 3) AS post2020_loss_ha,
       (coalesce(s.post2020_loss_ha, 0) > 0) AS follow_up_required,
       CASE
         WHEN p.resolved_cod_imovel IS NULL THEN p.resolution_status
         WHEN s.cod_imovel IS NULL THEN 'no_matched_fields'
         WHEN coalesce(s.relevant_field_count, 0) = 0 THEN 'no_relevant_commodity_fields'
         WHEN coalesce(s.post2020_loss_ha, 0) = 0 THEN 'no_post2020_loss'
         ELSE 'flagged_post2020_loss'
       END AS assessment_status,
       CASE s.dominant_mbmode24
         WHEN 15 THEN 'slaughter_point' WHEN 21 THEN 'slaughter_point'
         WHEN 35 THEN 'none' WHEN 39 THEN 'intake_point' WHEN 46 THEN 'none'
         WHEN 18 THEN 'intake_point, mill_point' WHEN 20 THEN 'mill_point'
         WHEN 41 THEN 'intake_point' ELSE 'none' END AS applicable_delivery_tiers,
       c.contact_entity_id, c.contact_entity_type, c.contact_tier,
       c.contact_evidence_basis, c.contact_evidence_value,
       round(c.contact_distance_m, 1) AS contact_distance_m,
       coalesce(c.facility_override, false) AS facility_override,
       CASE
         WHEN coalesce(s.post2020_loss_ha, 0) = 0 THEN 'not_applicable'
         WHEN c.contact_entity_id IS NULL THEN 'missing_membership_candidate'
         WHEN c.facility_override THEN 'nearby_routed_facility_override'
         WHEN s.dominant_mbmode24 IN (35, 46) THEN 'commodity_has_no_facility_coverage_default_membership'
         ELSE 'default_membership_no_qualifying_override'
       END AS contact_selection_status,
       coalesce(s.unsupported_class_field_count, 0) AS unsupported_class_field_count,
       p.geometry
FROM properties p LEFT JOIN property_field_summary s ON p.resolved_cod_imovel = s.cod_imovel
LEFT JOIN selected_contacts c ON p.resolved_cod_imovel = c.cod_imovel;

CREATE OR REPLACE TABLE audit_summary AS
SELECT 'input_rows' AS metric, count(*)::DOUBLE AS value FROM input_rows
UNION ALL SELECT 'unique_submissions', count(DISTINCT submission_key) FROM input_rows
UNION ALL SELECT 'unique_analysis_properties', count(*) FROM properties
UNION ALL SELECT 'axis_repaired_rows', count(*) FROM input_rows WHERE axes_repaired
UNION ALL SELECT 'resolved_input_rows', count(*) FROM property_resolution WHERE resolved_cod_imovel IS NOT NULL
UNION ALL SELECT 'unresolved_input_rows', count(*) FROM property_resolution WHERE resolved_cod_imovel IS NULL
UNION ALL SELECT 'matched_fields', count(*) FROM field_match_diagnostics WHERE matched
UNION ALL SELECT 'nearby_unmatched_fields', count(*) FROM field_match_diagnostics WHERE NOT matched
UNION ALL SELECT 'flagged_properties', count(*) FROM property_results WHERE follow_up_required
UNION ALL SELECT 'flagged_without_contact', count(*) FROM property_results WHERE follow_up_required AND contact_entity_id IS NULL;

COPY (SELECT * EXCLUDE geometry FROM property_results ORDER BY follow_up_required DESC, post2020_loss_ha DESC, property_key)
  TO 'property_results.csv' (HEADER, DELIMITER ',');
COPY (SELECT * FROM audit_summary) TO 'audit_summary.csv' (HEADER, DELIMITER ',');
COPY (SELECT * FROM property_resolution ORDER BY input_row) TO 'property_resolution.csv' (HEADER, DELIMITER ',');
COPY (SELECT cod_imovel, field_id, mbmode24, class_name, relevant_commodity, commodity,
             deforestarea2124 / 10000.0 AS post2020_loss_ha, field_area_m2 / 10000.0 AS field_area_ha,
             parcel_overlap_fraction
      FROM matched_fields ORDER BY cod_imovel, field_id)
  TO 'matched_fields.csv' (HEADER, DELIMITER ',');
COPY (SELECT * FROM property_results ORDER BY property_key)
  TO 'property_results.geojson' WITH (FORMAT GDAL, DRIVER 'GeoJSON', LAYER_CREATION_OPTIONS 'WRITE_BBOX=YES');
