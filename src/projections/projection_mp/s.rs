use super::*;
use crate::projections::projection_mp::structs::SModelPoint;

//---------------------------------------------------------------------------------------------------------
// PRIVATE
//---------------------------------------------------------------------------------------------------------
// ---------------------
// Intialize LazyFrame
// ---------------------
fn _initialize_lf(id: i32, term: i32, entry_age: i32, sum_insured: f64) -> PolarsResult<LazyFrame> {
    let length = (term * 12 + 1) as usize; // Total months in the term

    let lf = df![
        "id" => vec![id; length],
        "term" => vec![term; length],
        "sum_insured" => vec![sum_insured; length],
        "claim_pp" => vec![sum_insured; length], // Claim per policy is the sum insured
        "t" => (0..= (length - 1) as i32).map(|x| x as f64).collect::<Vec<f64>>(),
        "t_i32" => (0..= (length -1) as i32).collect::<Vec<i32>>(), // t_ is used for duration
    ]
    .unwrap()
    .lazy()
    .with_column((col("t_i32") / lit(12)).alias("duration"))
    .with_column((lit(entry_age) + col("duration")).alias("age"))
    .select([all().exclude(["t_i32"])]); // Drop t_i32 column

    Ok(lf)
}

// ---------------------
// Map assumptions
// ---------------------
// Map mortality assumption
fn __map_mort_assumption(
    lf: LazyFrame,
    mort_df: &DataFrame,
    gender: &str,
) -> PolarsResult<LazyFrame> {
    // Find the column name according to gender
    let suffix = format!("_{}", gender.to_lowercase());
    let mort_col_name = mort_df
        .get_column_names()
        .iter()
        .find(|&col| col.ends_with(&suffix))
        .ok_or_else(|| {
            PolarsError::ComputeError(
                format!("Mortality column with suffix '{suffix}' not found").into(),
            )
        })?
        .as_str();

    // Convert to LazyFrame, selecting columns and renmaing the mortality rate column
    let mort_lf = mort_df
        .clone()
        .lazy()
        .select([col("age"), col(mort_col_name).alias("mort_rate")]);

    // Left join with the mortality rate - similar to vlookup in Excel
    let lf = lf
        .left_join(mort_lf, col("age"), col("age"))
        .with_column(col("mort_rate").fill_null(lit(0.0)).alias("mort_rate"));

    Ok(lf)
}

// Map lapse, Inflation, Expenses and Spot rate assumption
fn __map_other_assumption(lf: LazyFrame, lookup_df: &DataFrame) -> PolarsResult<LazyFrame> {
    let col_name = lookup_df.get_column_names()[1].as_str();

    let lookup_lf = lookup_df
        .clone()
        .lazy()
        .with_column((col("year") - lit(1)).alias("duration")) // Adjust year to duration
        .select([col("duration"), col(col_name)]); // Drop "year" column

    let lf = lf
        .left_join(lookup_lf, col("duration"), col("duration"))
        .with_column(col(col_name).fill_null(lit(0.0)).alias(col_name)); // Fill null with 0.0

    Ok(lf)
}

fn _map_assumptions(
    lf: LazyFrame,
    assumptions: &AssumptionScenario,
    gender: &str,
) -> PolarsResult<LazyFrame> {
    // Map mortality assumption based
    let lf = __map_mort_assumption(lf, &assumptions.mort, gender)?;

    // Map other assumptions by iterating over each field of the AssumptionSet struct
    let lf = __map_other_assumption(lf, &assumptions.lapse)?;
    let lf = __map_other_assumption(lf, &assumptions.acq)?;
    let lf = __map_other_assumption(lf, &assumptions.mtn)?;
    let lf = __map_other_assumption(lf, &assumptions.inf)?;
    let lf = __map_other_assumption(lf, &assumptions.spot)?;
    let lf = __map_other_assumption(lf, &assumptions.load)?;

    Ok(lf)
}

// ---------------------
// Discount factor
// ---------------------
fn _discount_factor(lf: LazyFrame) -> PolarsResult<LazyFrame> {
    let lf = lf
        .with_column(
            // Spot rate monthly
            ((lit(1.0) + col("spot_rate")).pow(1.0 / 12.0) - lit(1.0)).alias("spot_rate_mth"),
        )
        .with_column(
            // Discount factor
            (lit(1.0) / (lit(1.0) + col("spot_rate_mth")).pow(col("t"))).alias("discount_factor"),
        );

    Ok(lf)
}

// ---------------------
// Inflation factor
// ---------------------
fn _inflation_factor(lf: LazyFrame) -> PolarsResult<LazyFrame> {
    let lf = lf.with_column(
        // Inflation factor - for flat curve only
        (lit(1.0) + col("inf_rate"))
            .pow(col("t") / lit(12.0))
            .alias("inf_factor"),
    );

    Ok(lf)
}

// Expense per policy
fn _exp_pp(lf: LazyFrame) -> PolarsResult<LazyFrame> {
    let lf = lf
        .with_columns(vec![
            // Total real expense per policy
            (col("real_acq_exp_pp") + col("real_mtn_exp_pp")).alias("real_exp_pp"),
            // Inflation factor - for flat curve only
            (lit(1.0) + col("inf_rate"))
                .pow(col("t") / lit(12.0))
                .alias("inf_factor"),
        ])
        .with_column(
            // Adjusted expense per policy
            (col("real_exp_pp") * col("inf_factor")).alias("exp_pp"),
        );

    Ok(lf)
}

// ---------------------
// Policy movement
// ---------------------
fn _policies_movement(lf: LazyFrame, policy_count: f64, term: i32) -> PolarsResult<LazyFrame> {
    let lf = lf.with_columns(vec![
        // Monthly decrement rate
        (lit(1.0) - (lit(1.0) - col("mort_rate")).pow(1.0 / 12.0)).alias("mort_rate_mth"),
        (lit(1.0) - (lit(1.0) - col("lapse_rate")).pow(1.0 / 12.0)).alias("lapse_rate_mth"),
    ]);

    let df = lf.clone().collect()?;

    // Height of the dataframe
    let height = df.height() as usize;

    // Monthly mortality and lapse rate
    let mort_rate_mth = df.column("mort_rate_mth")?.f64()?.to_vec();
    let lapse_rate_mth = df.column("lapse_rate_mth")?.f64()?.to_vec();

    // Create a vector of 0.0 with length equal to lf.height()
    let mut pols_if = Array1::<f64>::zeros(height).to_vec();
    pols_if[0] = policy_count; // Set first element to policy_count

    let mut pols_maturity = Array1::<f64>::zeros(height).to_vec();
    let mut pols_death = Array1::<f64>::zeros(height).to_vec();
    let mut pols_lapse = Array1::<f64>::zeros(height).to_vec();

    for i in 0..(height - 1) {
        if i == 0 {
            pols_if[i] = policy_count;
        } else {
            pols_if[i] =
                pols_if[i - 1] - pols_maturity[i - 1] - pols_death[i - 1] - pols_lapse[i - 1];
        }

        pols_maturity[i] = if i == (term * 12) as usize {
            pols_if[i] // Maturity at the end of the term
        } else {
            0.0 // No maturity before term ends
        };

        pols_death[i] = (pols_if[i] - pols_maturity[i]) * mort_rate_mth[i].unwrap_or(0.0);
        pols_lapse[i] =
            (pols_if[i] - pols_maturity[i] - pols_death[i]) * lapse_rate_mth[i].unwrap_or(0.0);
    }

    // Create a DataFrame from these vectors
    let new_df = df![
        "pols_if" => pols_if,
        "pols_maturity" => pols_maturity,
        "pols_death" => pols_death,
        "pols_lapse" => pols_lapse,
    ]?;

    // Horizontally concatenate the new columns to the existing LazyFrame
    let lf = df
        .hstack(&[
            new_df.column("pols_if")?.clone(),
            new_df.column("pols_maturity")?.clone(),
            new_df.column("pols_death")?.clone(),
            new_df.column("pols_lapse")?.clone(),
        ])?
        .lazy();

    Ok(lf)
}

// ---------------------
// Complete projection
// ---------------------
fn __calculate_net_premium(lf: LazyFrame) -> PolarsResult<f64> {
    // Calculate both PV claims and premium annuities in a single operation
    let lf = lf
        .with_column(
            // Portfolio claims
            (col("claim_pp") * col("pols_death")).alias("claims"),
        )
        .with_columns(vec![
            // PV of claims and PV of premium annuities
            (col("claims") * col("discount_factor")).alias("pv_claims_component"),
            (col("pols_if") * col("discount_factor")).alias("pv_annuities_component"),
        ])
        .select([
            col("pv_claims_component").sum().alias("pv_claims"),
            col("pv_annuities_component").sum().alias("prem_annuities"),
        ])
        .collect()?;

    // Extract both values in one operation
    let pv_claims = lf
        .column("pv_claims")?
        .get(0)?
        .extract::<f64>()
        .unwrap_or(0.0);

    let prem_annuities = lf
        .column("prem_annuities")?
        .get(0)?
        .extract::<f64>()
        .unwrap_or(0.0);

    // Calculate net premium
    let net_premium = if prem_annuities != 0.0 {
        pv_claims / prem_annuities
    } else {
        0.0
    };

    Ok(net_premium)
}

// Complete projection
fn _complete_projection(lf: LazyFrame) -> PolarsResult<LazyFrame> {
    // Calculate net premium
    let net_prem = __calculate_net_premium(lf.clone())?;

    // Add net premium to the lazyframe
    let lf = lf
        .with_columns(vec![
            // Loaded premium and round to 2 decimal places
            ((lit(1.0) + col("load_rate")) * lit(net_prem)).alias("prem_pp"),
            // Portfolio expense
            (col("exp_pp") * col("pols_if")).alias("expenses"),
        ])
        .with_columns(vec![
            // Portfolio claims
            (col("claim_pp") * col("pols_death")).alias("claims"),
            // Portfolio premiums
            (col("prem_pp") * col("pols_if")).alias("premiums"),
        ])
        .with_column(
            // Features is simple - Comission is 100% of premium in the first year
            when(col("duration").eq(0))
                .then(col("premiums"))
                .otherwise(lit(0.0))
                .alias("commissions"),
        )
        .with_column(
            (col("premiums") - col("expenses") - col("claims") - col("commissions"))
                .alias("net_cf"),
        );

    Ok(lf)
}

//---------------------------------------------------------------------------------------------------------
// PUBLIC
//---------------------------------------------------------------------------------------------------------
pub fn project_s_mp(mp: SModelPoint, assumptions: &AssumptionScenario) -> PolarsResult<LazyFrame> {
    // Initialize projection dataframe - using all interger values
    let lf = _initialize_lf(mp.id, mp.term, mp.entry_age, mp.sum_insured)?;

    // Map assumptions
    let lf = _map_assumptions(lf, assumptions, &mp.gender)?;

    // Perform projection
    let lf = _discount_factor(lf)?;
    let lf = _exp_pp(lf)?;
    let lf = _policies_movement(lf, mp.policy_count, mp.term)?;
    let lf = _complete_projection(lf)?;

    Ok(lf)
}
