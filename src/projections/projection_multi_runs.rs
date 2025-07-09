use polars::prelude::*;
use rayon::prelude::*;
use std::fs::{read_to_string, write};
use std::path::Path;

use crate::projections::helpers::create_folder;
use crate::projections::projection_single_run::{SingleRunResult, SingleRunSetup};

//---------------------------------------------------------------------------------------------------------
// STRUCTS
//---------------------------------------------------------------------------------------------------------

//--------------------
// Setup
//--------------------
#[derive(Clone, Debug)]
pub struct RunsSetup {
    pub description: String,         // Optional description for the run
    pub setups: Vec<SingleRunSetup>, // Vector of run setups
}

#[allow(dead_code)]
impl RunsSetup {
    pub fn get_run_setup_count(&self) -> usize {
        self.setups.len()
    }

    pub fn get_run_setup(&self, run_id: usize) -> Option<&SingleRunSetup> {
        self.setups.get(run_id)
    }

    pub fn projection_runs(&self) -> PolarsResult<RunsResult> {
        _project_runs(&self.setups, Some(&self.description))
    }
}

//--------------------
// Result
//--------------------
#[derive(Clone, Debug)]
pub struct RunsResult {
    pub description: String,           // Inherit from RunsSetup
    pub results: Vec<SingleRunResult>, // Vector of run results
}

#[allow(dead_code)]
impl RunsResult {
    pub fn get_run_result_count(&self) -> usize {
        self.results.len()
    }

    pub fn get_run_result(&self, run_id: usize) -> Option<&SingleRunResult> {
        self.results.get(run_id)
    }

    pub fn aggregate_projection_df(&self) -> PolarsResult<DataFrame> {
        // Concatenate all projected DataFrames from the results
        let mut all_lfs = Vec::with_capacity(self.results.len());

        for (i, result) in self.results.iter().enumerate() {
            let df = result.projected_df.clone();
            // Add run_id and run_setup_description columns, assumption_scenario.name
            let lf = df.lazy().with_columns(vec![
                lit(i as i32).alias("run_id"),
                lit(result.setup.description.clone()).alias("run_setup_description"),
                lit(result.setup.assumption_scenario.name.clone())
                    .alias("run_setup_assumptions_name"),
            ]);

            all_lfs.push(lf);
        }

        // Concatenate all DataFrames into one
        let aggregated_df = concat(all_lfs, Default::default())?.collect()?;
        Ok(aggregated_df)
    }

    pub fn export(&self, folder_path_str: &str) -> PolarsResult<()> {
        let vec_len = self.results.len();

        // Create the folder for the run result - if not given a name, use a UUID
        let path = Path::new(&folder_path_str);

        // Create the folder if it does not exist
        create_folder(path);

        // Export description
        let description_content = serde_json::json!({
            "description": self.description,
            "runs_count": vec_len
        })
        .to_string();

        let info_path = path.join("info.json");

        write(&info_path, description_content)?;

        // Loop over each run result and export it
        for (i, result) in self.results.iter().enumerate() {
            let run_path = path.join(format!("run_{i}")); // Folder containing each run result seperately
            create_folder(&run_path); // Create the folder for the run
            result.export(&run_path.to_str().unwrap())?; // Export run setup
        }

        Ok(())
    }

    pub fn import(folder_path_str: &str) -> PolarsResult<Self> {
        let path = Path::new(folder_path_str);

        // Check if the folder exists
        if !path.exists() || !path.is_dir() {
            return Err(PolarsError::ComputeError(
                format!("Folder does not exist: {folder_path_str}").into(),
            ));
        }

        // Import run setup
        let info = path.join("info.json");

        let info_content = read_to_string(info)?;

        let info_json: serde_json::Value = serde_json::from_str(&info_content)
            .map_err(|e| PolarsError::ComputeError(format!("serde_json error: {e}").into()))?;

        let description = info_json["description"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let vec_len = info_json["runs_count"].as_u64().unwrap_or(0) as usize;

        // Import projected DataFrame
        let mut results = Vec::with_capacity(vec_len);
        for i in 0..vec_len {
            let run_path = path.join(format!("run_{i}"));
            let result = SingleRunResult::import(run_path.to_str().unwrap())?;
            results.push(result);
        }

        let result = RunsResult {
            description,
            results,
        };

        Ok(result)
    }
}

//---------------------------------------------------------------------------------------------------------
// PRIVATE
//---------------------------------------------------------------------------------------------------------
/*
Using the below command to run the code in parallel with limited threads finish run in 90s vs 400s in non parallel mode
The test is not exhaustive, but it shows that parallel processing can significantly speed up the projection of model points.
$env:RAYON_NUM_THREADS = 8; $env:RUST_MIN_STACK = 33554432; cargo run
*/
fn _project_runs(
    setups: &Vec<SingleRunSetup>,
    description: Option<&str>,
) -> PolarsResult<RunsResult> {
    let results = setups
        .par_iter()
        .map(|setup| setup.projection_run())
        .collect::<PolarsResult<Vec<SingleRunResult>>>()?;

    let runs = RunsResult {
        description: description.unwrap_or_default().to_string(),
        results,
    };

    Ok(runs)
}
