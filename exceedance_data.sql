
-- Put together a list of permits to include
DROP TABLE IF EXISTS tmp_synth_permits CASCADE;
CREATE LOCAL TEMPORARY TABLE tmp_synth_permits
    ON COMMIT PRESERVE ROWS
    AS
WITH permits_pre AS (
    SELECT npdes_permit_id, facility_type_indicator,
            actual_average_flow_nmbr,
            row_number() OVER (partition BY npdes_permit_id ORDER BY CASE WHEN facility_type_indicator = 'POTW' THEN 1 ELSE 0 END DESC, RANDOM()) AS rn
    FROM icis.permits p
    LEFT JOIN scratch.treated_ca_permits r using(npdes_permit_id)
    WHERE       
      -- Conditions for a permit to be counted
      p.individual_permit_flag = 1
        -- Remove territories & DC
      AND NOT p.permit_state IN (
        'AS',
        'DC',
        'GE',
        'GU',
        'GM',
        'MP',
        'MW',
        'NN',
        'PR',
        'SR',
        'VI'
      )
      -- Remove CA permittees in non-treatment regions
      AND (r.npdes_permit_id IS NOT NULL OR p.permit_state != 'CA')
)
SELECT * FROM permits_pre WHERE rn = 1;
ALTER TABLE tmp_synth_permits ADD PRIMARY KEY(npdes_permit_id);

-- Create final output data table 
DROP TABLE IF EXISTS scratch.synth_exceedance_data CASCADE;
CREATE TABLE scratch.synth_exceedance_data
    AS
    -- Collect DMRs meeting our basic inclusion criteria, assigning them a parameter category
    -- and time period, and calculating exceedance %.
WITH dmr_data_pre AS (
    SELECT 
      d.permit_state,
      d.npdes_permit_id,
      p.actual_average_flow_nmbr,
      d.monitoring_period_end_date,
      d.perm_feature_nmbr,
      --Collect late submissions
      (CASE WHEN d.days_late IS NULL THEN 0 
            -- Exploration suggests that days_late=99 is used as a filler value for something else,
            -- so we remove it
            WHEN d.days_late = 99 THEN NULL 
            ELSE d.days_late END) AS days_late,
      -- Assign each DMR to a time period
      DATE_TRUNC('year', d.monitoring_period_end_date) AS year,
      -- (CASE WHEN EXTRACT(QUARTER FROM d.monitoring_period_end_date) > 2 THEN 1 ELSE 0 END) AS halfyear,
      -- Create parameter category
      COALESCE(pc.parameter_category, 'Other') AS parameter_category,
      statistical_base_type_code,
      nmbr_of_submission,
      (
        (
          CAST(dmr_value_standard_units AS double precision) - CAST(limit_value_standard_units AS double precision)
        ) * 100 * limit_value_qualifier_factor / CAST(limit_value_standard_units AS double precision)
      ) AS exceedance_pct_A
    FROM
      icis.dmrs d
      -- Join in permit data in order to omit general permittees
      -- and non-POTWs
      JOIN tmp_synth_permits p USING(npdes_permit_id)
      LEFT JOIN scratch.param_categories pc USING(parameter_code)
    WHERE
      -- Conditions for a DMR to be counted:
      d.monitoring_period_end_date <= '2016-12-31'
      AND d.monitoring_period_end_date >= '2004-01-01'
      AND d.optional_monitoring_flag = 'N'
      AND d.limit_type_code = 'ENF'
      AND d.parameter_desc != 'pH'
      AND NOT d.limit_unit_desc IN (
        'severity',
        'diverse index',
        'threshold #',
        'table #',
        'abst=0;prst=1',
        'prst=0;abst=1',
        'pass=0;fail=1',
        'fail=0;pass=1',
        'tox chronic',
        'toxic',
        'tox acute',
        'ebb/flood',
        'state class',
        'Ratio',
        'date',
        'Y=0;N=1',
        'N=0;Y=1',
        'op info',
        'low/high'
      )
      AND (
        d.dmr_value_qualifier_code = '='
        OR d.dmr_value_qualifier_code IS NULL
      ) 
        -- Omit all DMR values with limit = 0, as exceedance % cannot be 
        -- calculated in these cases
      AND CAST(d.limit_value_standard_units AS double precision) != 0
      AND NOT d.dmr_value_standard_units IS NULL
      AND NOT d.limit_value_standard_units IS NULL
      
      -- Include only POTWs
      AND p.facility_type_indicator = 'POTW'
)
  -- From these DMRs, calculate alternative outcome variables
, dmr_data AS (
    SELECT
      *,
      --Calculating exceedance % B
      (CASE
        WHEN exceedance_pct_A > 0 THEN exceedance_pct_A
        ELSE 0 END
      ) AS exceedance_pct_B,
      --Calculating exceedance % A (Maxes and mins only)
      ( CASE WHEN statistical_base_type_code in ('MAX', 'MIN') THEN
        exceedance_pct_A
        ELSE NULL END
      ) AS exceedance_pct_A_maxmin,
      --Calculating exceedance % B (Maxes and mins only)
      (CASE
        WHEN statistical_base_type_code in ('MAX', 'MIN') AND exceedance_pct_A > 0 THEN exceedance_pct_A
        WHEN statistical_base_type_code in ('MAX', 'MIN') THEN 0
        ELSE NULL
      END) AS exceedance_pct_B_maxmin,
      (CASE WHEN exceedance_pct_A > 0 THEN 1 ELSE 0 END) AS any_exceedance,
      (CASE WHEN exceedance_pct_A > 20 THEN 1 ELSE 0 END) AS any_exceedance_20pct,
      (CASE WHEN exceedance_pct_A > 40 THEN 1 ELSE 0 END) AS any_exceedance_40pct
    FROM dmr_data_pre
)
  -- Trim extreme exceedance % values, except for select parameter categories
, cleaned_dmr_data AS (
    SELECT *
    FROM dmr_data
    WHERE
    -- Trim extreme exceedance % values that are likely data integrity issues
    (exceedance_pct_A < 1000
    AND exceedance_pct_A > -1000) OR 
    (parameter_category IN ('TSS', 'TRC', 'Pathogens'))
)
  -- Aggregate facility data over time, winsorizing remaining extreme exceedance %s
, data_by_facility_period AS (
    SELECT
        permit_state,
        npdes_permit_id,
        actual_average_flow_nmbr,
        parameter_category,
        year,
        -- halfyear,
        AVG((CASE WHEN exceedance_pct_A > 1000 THEN 1000 ELSE exceedance_pct_A END)) AS mean_exceedance_pct_A,
        AVG((CASE WHEN exceedance_pct_B > 1000 THEN 1000 ELSE exceedance_pct_B END)) AS mean_exceedance_pct_B,
        AVG((CASE WHEN exceedance_pct_A_maxmin > 1000 THEN 1000 ELSE exceedance_pct_A_maxmin END)) AS mean_exceedance_pct_A_maxmin,
        AVG((CASE WHEN exceedance_pct_B_maxmin > 1000 THEN 1000 ELSE exceedance_pct_B_maxmin END)) AS mean_exceedance_pct_B_maxmin,
        MAX(any_exceedance) AS any_exceedance,
        MAX(any_exceedance_20pct) AS any_exceedance_20pct,
        MAX(any_exceedance_40pct) AS any_exceedance_40pct,
        AVG(any_exceedance) AS exceedance_rate,
        AVG(any_exceedance_20pct) AS exceedance_20pct_rate,
        AVG(any_exceedance_40pct) AS exceedance_40pct_rate,
        AVG(days_late) AS mean_days_late,
        SUM(any_exceedance) AS n_exceedances,
        COUNT(DISTINCT npdes_permit_id || monitoring_period_end_date || perm_feature_nmbr) AS n_dmrs,
        SUM((
          CASE
            WHEN nmbr_of_submission = 1 THEN 1
            ELSE 0
          END
        )) AS n_monthly,
        SUM((
          CASE
            WHEN nmbr_of_submission = 3 THEN 1
            ELSE 0
          END
        )) AS n_quarterly,
        SUM((
          CASE
            WHEN nmbr_of_submission = 6 THEN 1
            ELSE 0
          END
        )) AS n_semiannual,
        SUM((
          CASE
            WHEN nmbr_of_submission = 12 THEN 1
            ELSE 0
          END
        )) AS n_annual,
        SUM((CASE WHEN statistical_base_type_code = 'AVG' THEN 1 ELSE 0 END)) AS n_AVG,
        SUM((CASE WHEN statistical_base_type_code = 'MIN' THEN 1 ELSE 0 END)) AS n_MIN,
        SUM((CASE WHEN statistical_base_type_code = 'MAX' THEN 1 ELSE 0 END)) AS n_MAX
    FROM cleaned_dmr_data
    GROUP BY 
      GROUPING SETS( 
      (permit_state, parameter_category, npdes_permit_id, actual_average_flow_nmbr, year),
      (permit_state, npdes_permit_id, actual_average_flow_nmbr, year)
      )
)

-- Into the final data table, insert data now aggregated over facilities.
SELECT 
  permit_state,
  parameter_category,
  year, 
  -- halfyear,
  AVG(mean_exceedance_pct_A) AS mean_exceedance_pct_A,
  AVG(mean_exceedance_pct_B) AS mean_exceedance_pct_B,
  AVG(mean_exceedance_pct_A_maxmin) AS mean_exceedance_pct_A_maxmin,
  AVG(mean_exceedance_pct_B_maxmin) AS mean_exceedance_pct_B_maxmin,
  AVG(any_exceedance) AS frac_any_exceedance,
  AVG(any_exceedance_20pct) AS frac_any_exceedance_20pct,
  AVG(any_exceedance_40pct) AS frac_any_exceedance_40pct,
  AVG(exceedance_rate) AS avg_exceedance_rate,
  AVG(exceedance_20pct_rate) AS avg_exceedance_20pct_rate,
  AVG(exceedance_40pct_rate) AS avg_exceedance_40pct_rate,
  AVG(mean_days_late) AS mean_days_late,
  COUNT(DISTINCT npdes_permit_id) AS n_permittees,
  SUM(n_exceedances) AS n_exceedances,
  SUM(n_dmrs) AS n_dmrs,
  SUM(n_monthly) AS n_monthly,
  SUM(n_quarterly) AS n_quarterly,
  SUM(n_semiannual) AS n_semiannual,
  SUM(n_annual) AS n_annual,
  SUM(n_AVG) AS n_AVG,
  SUM(n_MIN) AS n_MIN,
  SUM(n_MAX) AS n_MAX,
  PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY actual_average_flow_nmbr) AS med_actual_average_flow_nmbr,
  MIN(actual_average_flow_nmbr) AS min_actual_average_flow_nmbr,
  MAX(actual_average_flow_nmbr) AS max_actual_average_flow_nmbr
FROM data_by_facility_period
GROUP BY 
  permit_state,
  parameter_category,
  year
;
COMMIT
;
SELECT * FROM scratch.synth_exceedance_data
;