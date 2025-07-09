use crate::assumptions::helpers::{
    get_indices_names_hashmap, get_sheet_by_name, parse_col_by_index_to_f64,
    parse_col_by_index_to_i32,
};
use polars::prelude::*;
use std::collections::VecDeque;

//---------------------------------------------------------------------------------------------------------
// PRIVATE
//---------------------------------------------------------------------------------------------------------
fn _get_assumption_df(
    sheet_name: &str,
    col_names: &[&str],
    new_col_names: Option<&[&str]>,
) -> PolarsResult<DataFrame> {
    // If new_col_names is provided, ensure it matches the length of col_names
    if let Some(new_names) = new_col_names {
        if col_names.len() != new_names.len() {
            return Err(PolarsError::ComputeError(
                "Length of col_names and new_col_names must match".into(),
            ));
        }
    }

    // Find the sheet index by name
    let sheet = get_sheet_by_name(sheet_name)?;

    // Get indices for requested columns (excluding first column)
    let col_hashmap = get_indices_names_hashmap(&sheet, col_names, new_col_names)?;

    let mut series_vec = VecDeque::new();

    for (col_idx, (_, new_name)) in col_hashmap.iter() {
        // First column is always i32
        if *col_idx == 0 {
            let col_data_i32: Vec<i32> = parse_col_by_index_to_i32(&sheet, *col_idx)?;
            let col = Series::new(new_name.into(), col_data_i32).into_column();
            series_vec.push_front(col)
        } else {
            let col_data_f64: Vec<f64> = parse_col_by_index_to_f64(&sheet, *col_idx)?;
            let col = Series::new(new_name.into(), col_data_f64).into_column();
            series_vec.push_back(col);
        };
    }

    DataFrame::new(series_vec.into())
}

//---------------------------------------------------------------------------------------------------------
// PUBLIC
//---------------------------------------------------------------------------------------------------------
// Mortality assumption: The schema is slightly different from other since it is based on gender
pub fn get_mort_rate_df(mort_name: &str) -> PolarsResult<DataFrame> {
    let col1 = format!("{mort_name}_m");
    let col2 = format!("{mort_name}_f");
    let col_names = [col1.as_str(), col2.as_str()];
    let df = _get_assumption_df("mort_rate", &col_names, Some(&["mort_m", "mort_f"]))?;
    Ok(df)
}

// Lapse assumption
pub fn get_lapse_rate_df(lapse_name: &str) -> PolarsResult<DataFrame> {
    let df = _get_assumption_df("lapse_rate", &[lapse_name], Some(&["lapse_rate"]))?;
    Ok(df)
}

// Inflation assumption
pub fn get_inf_rate_df(inf_name: &str) -> PolarsResult<DataFrame> {
    let df = _get_assumption_df("inf_rate", &[inf_name], Some(&["inf_rate"]))?;
    Ok(df)
}

// Acquisition assumption
pub fn get_acq_exp_df(acq_exp_name: &str) -> PolarsResult<DataFrame> {
    let df = _get_assumption_df("acq_exp", &[acq_exp_name], Some(&["real_acq_exp_pp"]))?;
    Ok(df)
}

// Maintenance assumption
pub fn get_mtn_exp_df(mtn_exp_name: &str) -> PolarsResult<DataFrame> {
    let df = _get_assumption_df("mtn_exp", &[mtn_exp_name], Some(&["real_mtn_exp_pp"]))?;
    Ok(df)
}

pub fn get_spot_rate_df(spot_rate_name: &str) -> PolarsResult<DataFrame> {
    let df = _get_assumption_df("spot_rate", &[spot_rate_name], Some(&["spot_rate"]))?;
    Ok(df)
}

pub fn get_load_rate_df(load_rate_name: &str) -> PolarsResult<DataFrame> {
    let df = _get_assumption_df("load_rate", &[load_rate_name], Some(&["load_rate"]))?;
    Ok(df)
}

//---------------------------------------------------------------------------------------------------------
// UNIT TESTS
//---------------------------------------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fn_get_mort_df() {
        // Test reading data from the lapse_rate sheet
        let df = get_mort_rate_df("cso80");

        println!("{df:?}");

        assert!(true, "Assumption DataFrame should be created successfully");
    }
}
