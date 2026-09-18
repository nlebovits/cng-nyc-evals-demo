# Deforestation risk assessment

Assess rural properties for post-2020 deforestation risk and identify the best follow-up contact for each flagged property.

## Data and property resolution

* **Trazo3 fields:** `https://data.source.coop/wri-data-lab/trazofields/trazo3-fields/trazo3_brazil_goias_2024.parquet`
* **CAR parcels:** `https://data.source.coop/tristangruppwri/cadastral/brazil-car-area-imovel/brazil_car_area_imovel.parquet`
* **Commodity infrastructure:** `https://data.source.coop/tristangruppwri/soft-commodity-infrastructure/facilities/BR_facilities.parquet`

Geometry is WGS84 longitude/latitude. Trazo3 loss values are square metres; divide by 10,000 for hectares.

Resolve every input property to CAR. Analyze identical duplicates once, but account for every input row. Resolve points by containment and ID-less polygons by geometric match. Repair swapped axes only when the original coordinates fall outside Brazil and swapping them moves the geometry inside Brazil. Never silently drop unresolved or missing properties.

## Field matching and risk

A field matches the portfolio when either:

* ≥ `0.667` of its area lies inside one listed CAR parcel; or
* ≥ `0.667` lies inside the dissolved union of listed parcels buffered by `25 m`.

The buffer closes small gaps between neighboring parcels; dissolve before intersection to avoid double-counting overlap. Assign each matched field to the parcel containing the largest fraction of its area; break ties by lowest `cod_imovel`.

Use the Trazo3 **2021–2024** band as post-2020 loss. Any value > 0 counts.

| `mbmode24` | Class                 | Commodity | Relevant | Delivery tier                |
| ---------- | --------------------- | --------- | -------- | ---------------------------- |
| 15         | Pasture               | cattle    | yes      | `slaughter_point`            |
| 21         | Mosaic of Uses        | cattle    | yes      | `slaughter_point`            |
| 35         | Palm Oil              | oil palm  | yes      | none                         |
| 39         | Soybean               | soya      | yes      | `intake_point`               |
| 46         | Coffee                | coffee    | yes      | none                         |
| 9          | Forest Plantation     | wood      | no*      |                              |
| 18         | Agriculture           |           | no       | `intake_point`, `mill_point` |
| 20         | Sugarcane             |           | no       | `mill_point`                 |
| 40         | Rice                  |           | no       |                              |
| 41         | Other Temporary Crops |           | no       | `intake_point`               |
| 47         | Citrus                |           | no       |                              |
| 48         | Other Perennial       |           | no       |                              |
| 62         | Cotton                |           | no       |                              |

Classes absent from the table are outside the relevant commodity set.

* Forest Plantation is excluded because the detection product is unreliable for this class, not because planted timber is outside the underlying commodity framework.

Flag a property when at least one matched field in the relevant commodity set has post-2020 loss. Sum post-2020 loss across all relevant matched fields on the property.

Determine the property's dominant class from total matched-field area using **all** matched fields. Break area ties by lowest `mbmode24`.

## Follow-up contact

Every flagged property has a `membership_muni` cooperative candidate based on município; this is the default top contact.

Use the dominant class to determine applicable delivery infrastructure from the table above. Coffee and palm oil have no facility coverage; do not substitute another facility type.

A routed `intake_point` or `slaughter_point` less than `10 km` from the parcel centroid overrides the município candidate. `mill_point` never triggers this override.

Calculate facility distance in EPSG:5880. If multiple facilities qualify, rank by:

1. tier;
2. distance ascending;
3. evidence value descending;
4. entity ID ascending.

Retain the selected entity ID, entity type, tier, evidence basis, and distance where applicable.

## Auditability

Keep the analysis traceable to the input properties and source data. Make unresolved properties, missing CAR records, unmatched fields, unsupported commodities, missing infrastructure, and assumptions visible rather than silently omitting them.
