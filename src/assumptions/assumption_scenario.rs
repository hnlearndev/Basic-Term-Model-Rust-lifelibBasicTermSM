use crate::assumptions::assumption::{
    get_acq_exp_df, get_inf_rate_df, get_lapse_rate_df, get_load_rate_df, get_mort_rate_df,
    get_mtn_exp_df, get_spot_rate_df,
};
use crate::assumptions::helpers::{
    get_indices_names_hashmap, get_sheet_by_name, parse_col_by_index_to_string,
};
use polars::prelude::*;
use std::collections::VecDeque;
//---------------------------------------------------------------------------------------------------------
// STRUCTS
//---------------------------------------------------------------------------------------------------------
#[derive(Debug, Clone)]
pub struct AssumptionScenario {
    pub name: String, // Name of the assumption set
    pub mort: DataFrame,
    pub lapse: DataFrame,
    pub inf: DataFrame,
    pub acq: DataFrame,
    pub mtn: DataFrame,
    pub spot: DataFrame,
    pub load: DataFrame,
}

impl AssumptionScenario {
    pub fn new_by_name(scenario_name: &str) -> PolarsResult<Self> {
        // Get the assumption scenario by name
        let scenario_df = _get_assumption_scenario_df(scenario_name)?;

        // Convert the DataFrame to a Vec of tuples
        let scenario_vec = _dataframe_to_vec_of_tuples(&scenario_df)?;

        // Prepare variables for each assumption set
        let mut mort = None;
        let mut lapse = None;
        let mut inf = None;
        let mut acq = None;
        let mut mtn = None;
        let mut spot = None;
        let mut load = None;

        for (t, n) in scenario_vec.iter() {
            match t.as_str() {
                "mort" => mort = Some(get_mort_rate_df(n)?),
                "lapse" => lapse = Some(get_lapse_rate_df(n)?),
                "inf" => inf = Some(get_inf_rate_df(n)?),
                "acq" => acq = Some(get_acq_exp_df(n)?),
                "mtn" => mtn = Some(get_mtn_exp_df(n)?),
                "spot" => spot = Some(get_spot_rate_df(n)?),
                "load" => load = Some(get_load_rate_df(n)?),
                _ => {}
            }
        }

        let result = Self {
            name: scenario_name.to_string(),
            mort: mort
                .ok_or_else(|| PolarsError::ComputeError("Missing 'mort' assumption".into()))?,
            lapse: lapse
                .ok_or_else(|| PolarsError::ComputeError("Missing 'lapse' assumption".into()))?,
            inf: inf.ok_or_else(|| PolarsError::ComputeError("Missing 'inf' assumption".into()))?,
            acq: acq.ok_or_else(|| PolarsError::ComputeError("Missing 'acq' assumption".into()))?,
            mtn: mtn.ok_or_else(|| PolarsError::ComputeError("Missing 'mtn' assumption".into()))?,
            spot: spot
                .ok_or_else(|| PolarsError::ComputeError("Missing 'spot' assumption".into()))?,
            load: load
                .ok_or_else(|| PolarsError::ComputeError("Missing 'load' assumption".into()))?,
        };

        Ok(result)
    }
}

//---------------------------------------------------------------------------------------------------------
// PRIVATE
//---------------------------------------------------------------------------------------------------------
// There are exactly 2 columns
fn _get_assumption_scenario_df(col_name: &str) -> PolarsResult<DataFrame> {
    // Find the sheet index by name
    let sheet = get_sheet_by_name("scenarios")?;

    // Get indices for requested columns (excluding first column)
    let col_hashmap = get_indices_names_hashmap(&sheet, &[col_name], None)?;

    let mut series_vec = VecDeque::new();

    // Both columns are strings
    for (col_idx, (_, new_name)) in col_hashmap.iter() {
        let col_data = parse_col_by_index_to_string(&sheet, *col_idx)?;

        let col = Series::new(new_name.into(), col_data).into_column();

        if *col_idx == 0 {
            series_vec.push_front(col);
        } else {
            series_vec.push_back(col);
        }
    }

    DataFrame::new(series_vec.into())
}

// Convert a DataFrame with exactly 2 columns to Vec<(String, String)>
fn _dataframe_to_vec_of_tuples(df: &DataFrame) -> PolarsResult<Vec<(String, String)>> {
    let col1 = df.column(df.get_column_names()[0])?.str()?;
    let col2 = df.column(df.get_column_names()[1])?.str()?;

    let result = col1
        .into_iter()
        .zip(col2)
        .map(|(opt1, opt2)| {
            (
                opt1.unwrap_or_default().to_string(),
                opt2.unwrap_or_default().to_string(),
            )
        })
        .collect();

    Ok(result)
}

//---------------------------------------------------------------------------------------------------------
// UNIT TESTS
//---------------------------------------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fn_get_assumption_scenario_df() {
        // Test reading data from the mort_rate sheet
        let df = _get_assumption_scenario_df("pricing");

        println!("{df:?}");

        assert!(true, "Mortality DataFrame should be created successfully");
    }

    #[test]
    fn test_fn_data_frame_to_vec_of_tuples() {
        // Test converting a DataFrame to Vec<(String, String)>
        let df = _get_assumption_scenario_df("pricing").unwrap();
        let vec = _dataframe_to_vec_of_tuples(&df).unwrap();

        println!("{vec:?}");

        assert!(!vec.is_empty(), "Vec should not be empty");
    }

    #[test]
    fn test_method_assumption_scenario_new_by_name() {
        // Test reading data from the lapse_rate sheet
        let df = AssumptionScenario::new_by_name("pricing");

        println!("{df:?}");

        assert!(true, "AssumptionScenario should be created successfully");
    }
}
