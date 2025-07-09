use polars::prelude::*;
use spreadsheet_ods::{Sheet, read_ods};
use std::collections::HashMap;

//---------------------------------------------------------------------------------------------------------
// PRIVATE
//---------------------------------------------------------------------------------------------------------
fn _parse_cell_text(cell_str: &str) -> String {
    if cell_str.contains("Text(") {
        let start = cell_str.find("Text(").unwrap_or(0) + 5;
        let end = if let Some(style_pos) = cell_str.find("), style:") {
            style_pos
        } else {
            cell_str.rfind(")").unwrap_or(cell_str.len())
        };
        let extracted = &cell_str[start..end];
        // Remove quotes if present
        if extracted.starts_with('"') && extracted.ends_with('"') {
            extracted[1..extracted.len() - 1].to_string()
        } else {
            extracted.to_string()
        }
    } else {
        cell_str.to_string()
    }
}

fn _get_header_rows(sheet: &spreadsheet_ods::Sheet) -> Vec<String> {
    let mut header: Vec<String> = Vec::new();
    let mut col_idx = 0;

    while let Some(cell) = sheet.cell(0, col_idx) {
        let cell_str = format!("{cell:?}");
        let name = _parse_cell_text(&cell_str);
        header.push(name);
        col_idx += 1;
    }

    header
}

//---------------------------------------------------------------------------------------------------------
// PUBLIC
//---------------------------------------------------------------------------------------------------------
// The first column is always included in addtion to col
pub fn get_indices_names_hashmap(
    sheet: &Sheet,
    col_names: &[&str],
    new_col_names: Option<&[&str]>,
) -> PolarsResult<HashMap<usize, (String, String)>> {
    let mut indices = HashMap::new();

    // Get the header row (assume it's the first row)
    let header = _get_header_rows(sheet);

    // Always include the first column (index 0)
    if let Some(first_col_name) = header.first() {
        indices.insert(0, (first_col_name.clone(), first_col_name.clone()));
    }

    // Find indices for requested columns
    for (i, &col_name) in col_names.iter().enumerate() {
        let mut found = false;
        for (idx, name) in header.iter().enumerate() {
            if name == col_name {
                let new_name = if let Some(new_names) = new_col_names {
                    new_names
                        .get(i)
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| name.clone())
                } else {
                    name.clone()
                };
                indices.insert(idx, (name.clone(), new_name));
                found = true;
                break;
            }
        }
        if !found {
            return Err(PolarsError::ComputeError(
                format!("Column '{col_name}' not found in sheet header").into(),
            ));
        }
    }

    Ok(indices)
}

pub fn get_sheet_by_name(sheet_name: &str) -> PolarsResult<Sheet> {
    let doc = read_ods("src/assumptions/assumptions.ods")
        .map_err(|e| PolarsError::ComputeError(format!("Failed to read ODS file: {e}").into()))?;

    for idx in 0..doc.num_sheets() {
        let wsh = doc.sheet(idx);
        if wsh.name() == sheet_name {
            return Ok(wsh.clone());
        }
    }

    Err(PolarsError::ComputeError(
        format!("Sheet '{sheet_name}' not found").into(),
    ))
}

//--------------------------------------------
// Parse column by index to different types
//--------------------------------------------
pub fn parse_col_by_index_to_f64(
    sheet: &spreadsheet_ods::Sheet,
    col_idx: usize,
) -> PolarsResult<Vec<f64>> {
    let mut row_idx = 1; // Skip header

    let mut col_data = Vec::new();

    while let Some(cell_content) = sheet.cell(row_idx, col_idx as u32) {
        let cell_str = format!("{cell_content:?}");

        let val = if cell_str.contains("Number(") {
            let start = cell_str.find("Number(").unwrap_or(0) + 7;
            let end = cell_str.rfind(")").unwrap_or(cell_str.len());
            cell_str[start..end].parse::<f64>().unwrap_or(0.0)
        } else {
            0.0
        };
        col_data.push(val);
        row_idx += 1;
    }

    Ok(col_data)
}

pub fn parse_col_by_index_to_i32(sheet: &Sheet, col_idx: usize) -> PolarsResult<Vec<i32>> {
    // Convert to f64 first, then to i32
    let tmp_f64 = parse_col_by_index_to_f64(sheet, col_idx)?;
    let col_data_i32: Vec<i32> = tmp_f64.into_iter().map(|x| x as i32).collect();
    Ok(col_data_i32)
}

pub fn parse_col_by_index_to_string(sheet: &Sheet, col_idx: usize) -> PolarsResult<Vec<String>> {
    let mut row_idx = 1; // Skip header

    let mut col_data = Vec::new();

    while let Some(cell_content) = sheet.cell(row_idx, col_idx as u32) {
        let cell_str = format!("{cell_content:?}");

        // Extract the text value from the cell string
        let val = if cell_str.contains("Text(") {
            let start = cell_str.find("Text(").unwrap_or(0) + 5;

            let end = if let Some(style_pos) = cell_str.find("), style:") {
                style_pos
            } else {
                cell_str.rfind(")").unwrap_or(cell_str.len())
            };

            let extracted = &cell_str[start..end];

            // Remove quotes if present
            if extracted.starts_with('"') && extracted.ends_with('"') {
                extracted[1..extracted.len() - 1].to_string()
            } else {
                extracted.to_string()
            }
        } else {
            cell_str
        };
        col_data.push(val);
        row_idx += 1;
    }

    Ok(col_data)
}
