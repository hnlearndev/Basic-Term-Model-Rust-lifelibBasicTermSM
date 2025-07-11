mod assumptions;
mod mp_gen;
mod projections;

use crate::assumptions::assumption_scenario::AssumptionScenario;
use crate::mp_gen::pricing_mp_gen::generate_s_model_points; // Removed because function does not exist
use crate::projections::projection_multi_runs::RunsSetup;
use crate::projections::projection_single_run::SingleRunSetup;

use polars::prelude::*;
use std::time::Instant;

//---------------------------------------------------------------------------------------------------------
// PRIVATE
//---------------------------------------------------------------------------------------------------------
fn get_run_setups() -> PolarsResult<RunsSetup> {
    let model_points_df = generate_s_model_points()?;

    let run_setup_01 = SingleRunSetup {
        description: "Run setup 01 - Used for pricing".to_string(),
        model_points_df: model_points_df.clone(),
        assumption_scenario: AssumptionScenario::new_by_name("pricing")?,
    };

    let run_setup_02 = SingleRunSetup {
        description: "Run setup 02 - Used for valuation".to_string(),
        model_points_df: model_points_df.clone(),
        assumption_scenario: AssumptionScenario::new_by_name("valuation")?,
    };

    let result = RunsSetup {
        description: "Runs setup for pricing and valuation".to_string(),
        setups: vec![run_setup_01, run_setup_02],
    };

    Ok(result)
}

//---------------------------------------------------------------------------------------------------------
// MAIN
//---------------------------------------------------------------------------------------------------------
fn main() -> PolarsResult<()> {
    // Start timer
    let start = Instant::now();

    let run_setups = get_run_setups()?;

    let multi_run_results = run_setups.projection_runs()?;

    multi_run_results.export("src/results/first_test")?;

    // Print the time taken for the runs
    let duration = start.elapsed();
    println!("Time taken for runs: {duration:?}");

    Ok(())
}
