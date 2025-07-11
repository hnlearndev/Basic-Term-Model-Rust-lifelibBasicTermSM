use crate::assumptions::assumption_scenario::AssumptionScenario;
use crate::projections::projection_mp::ModelPoint;
use polars::prelude::*;
use rayon::prelude::*;
use std::fs::{File, read_to_string, write};
use std::path::Path;

use crate::projections::helpers::create_folder;

//---------------------------------------------------------------------------------------------------------
// STRUCTS
//---------------------------------------------------------------------------------------------------------

//--------------------
// Setup
//--------------------
#[derive(Clone, Debug)]
pub struct SingleRunSetup {
    pub description: String, // Optional description for the run
    pub model_points_df: DataFrame,
    pub assumption_scenario: AssumptionScenario,
}

#[allow(dead_code)]
impl SingleRunSetup {
    pub fn model_points_count(&self) -> usize {
        // Return the number of model points in the DataFrame
        self.model_points_df.height()
    }

    pub fn projection_run(&self) -> PolarsResult<SingleRunResult> {
        // Project the single run setup
        _project_single_run(self)
    }

    // Private: To serve as medium for run result
    fn export(&self, folder_path_str: &str) -> PolarsResult<()> {
        // Create the folder for the run result - if not given a name, use a UUID
        let path = Path::new(&folder_path_str);

        // Create the folder if it does not exist
        create_folder(path);

        // Export description & assumption scenario name as JSON
        let description_content = serde_json::json!({
            "description": self.description,
            "assumptions": self.assumption_scenario.name
        })
        .to_string();

        let info_path = path.join("info.json");
        write(&info_path, description_content)?;

        // Export model points DataFrame
        let model_points_path = path.join("model_points.parquet");
        let mut model_points_file = File::create(model_points_path)?;
        let mut model_points_df = self.model_points_df.clone();
        ParquetWriter::new(&mut model_points_file).finish(&mut model_points_df)?;

        Ok(())
    }

    fn import(folder_path_str: &str) -> PolarsResult<Self> {
        let path = Path::new(folder_path_str);

        // Check if the folder exists
        if !path.exists() || !path.is_dir() {
            return Err(PolarsError::ComputeError(
                format!("Folder does not exist: {folder_path_str}").into(),
            ));
        }

        // Import description
        let info_path = path.join("info.json");

        let info_content = read_to_string(&info_path)?;

        let info_json: serde_json::Value = serde_json::from_str(&info_content)
            .map_err(|e| PolarsError::ComputeError(format!("serde_json error: {e}").into()))?;

        let description = info_json["description"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        let assumptions_name = info_json["assumptions"]
            .as_str()
            .unwrap_or_default()
            .to_string();

        // Import model points DataFrame
        let model_points_path = path.join("model_points.parquet");
        let mut model_points_file = File::open(model_points_path)?;
        let model_points_df = ParquetReader::new(&mut model_points_file).finish()?;

        // Create the RunSetup instance
        let result = SingleRunSetup {
            description,
            model_points_df,
            assumption_scenario: AssumptionScenario::new_by_name(&assumptions_name)?,
        };

        Ok(result)
    }
}

//--------------------
// Result
//--------------------
#[derive(Clone, Debug)]
pub struct SingleRunResult {
    pub setup: SingleRunSetup,
    pub projected_df: DataFrame, // This is expensive procedure, so we store the result
}

#[allow(dead_code)]
impl SingleRunResult {
    pub fn export(&self, folder_path_str: &str) -> PolarsResult<()> {
        // Create the folder for the run result - if not given a name, use a UUID
        let path = Path::new(&folder_path_str);

        // Create the folder if it does not exist
        create_folder(path);

        // Export run_setup
        let setup_path = path.join("run_setup");
        self.setup.export(setup_path.to_str().unwrap())?;

        // Export projected DataFrame
        let projected_df_path = path.join("projected_df.parquet");
        let mut projected_df_file = File::create(projected_df_path)?;
        let mut projected_df = self.projected_df.clone();
        ParquetWriter::new(&mut projected_df_file).finish(&mut projected_df)?;

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
        let setup_path = path.join("run_setup");
        let setup = SingleRunSetup::import(setup_path.to_str().unwrap())?;

        // Import projected DataFrame
        let projected_df_path = path.join("projected_df.parquet");
        let projected_df = LazyFrame::scan_parquet(
            projected_df_path.to_str().unwrap(),
            ScanArgsParquet::default(),
        )?
        .collect()?;

        let result = SingleRunResult {
            setup,
            projected_df,
        };

        Ok(result)
    }
}

//---------------------------------------------------------------------------------------------------------
// PRIVATE
//---------------------------------------------------------------------------------------------------------
// Process data in chunks to avoid stack overflow
const CHUNK_SIZE: usize = 100;

fn _project_single_run(setup: &SingleRunSetup) -> PolarsResult<SingleRunResult> {
    // Convert model points DataFrame to vector
    let model_points_vec = __convert_model_points_df_to_vector(&setup.model_points_df)?;

    // Process chunks of model points in parallel with limited threads
    let chunks = model_points_vec
        .chunks(CHUNK_SIZE)
        .collect::<Vec<&[ModelPoint]>>();

    let all_chunk_lfs = chunks
        .into_par_iter()
        .map(|chunk| {
            // Process each chunk sequentially (no nested parallelism)
            let all_lfs = chunk
                .iter()
                .map(|mp| mp.project(&setup.assumption_scenario))
                .collect::<PolarsResult<Vec<LazyFrame>>>()?;

            // Concatenate LazyFrames within the chunk and collect to DataFrame
            let lf = concat(all_lfs, Default::default())?;

            Ok(lf)
        })
        .collect::<PolarsResult<Vec<LazyFrame>>>()?;

    // Concatenate all chunk DataFrames
    let final_lf = concat(all_chunk_lfs, Default::default())?;
    let final_df = final_lf.collect()?;

    // Return the result with run setup and projected DataFrame
    let result = SingleRunResult {
        setup: setup.clone(),
        projected_df: final_df,
    };

    Ok(result)
}

fn __convert_model_points_df_to_vector(df: &DataFrame) -> PolarsResult<Vec<ModelPoint>> {
    todo!("Implement conversion from DataFrame to Vec<ModelPoint>");
    // let id = df.column("id")?.i32()?;
    // let entry_age = df.column("entry_age")?.i32()?;
    // let gender = df.column("gender")?.str()?;
    // let term = df.column("term")?.i32()?;
    // let policy_count = df.column("policy_count")?.f64()?;
    // let sum_insured = df.column("sum_insured")?.f64()?;
    // let model = df.column("model")?.str()?;

    // let model_points = (0..df.height())
    //     .map(|i| ModelPoint {
    //         id: id.get(i).unwrap(),
    //         entry_age: entry_age.get(i).unwrap(),
    //         gender: gender.get(i).unwrap().to_string(),
    //         term: term.get(i).unwrap(),
    //         policy_count: policy_count.get(i).unwrap(),
    //         sum_insured: sum_insured.get(i).unwrap(),
    //         model: model.get(i).unwrap().to_string(),
    //     })
    //     .collect();

    // Ok(model_points)
}
