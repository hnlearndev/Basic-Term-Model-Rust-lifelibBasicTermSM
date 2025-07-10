use super::*;
use itertools::iproduct;

pub fn generate_s_model_points() -> PolarsResult<DataFrame> {
    let age: Vec<i32> = (20..60).collect();
    let term: Vec<i32> = vec![10, 15, 20];
    let gender: Vec<&str> = vec!["M", "F"];

    // Generate all combinations of age, term and gender
    let combinations: Vec<(i32, i32, &str)> = iproduct!(age.iter(), term.iter(), gender.iter())
        .map(|(&a, &t, &g)| (a, t, g))
        .collect();

    let model: Vec<&str> = vec!["s_model"; combinations.len()];
    let id: Vec<i32> = (1..=combinations.len() as i32).collect();
    let age: Vec<i32> = combinations.iter().map(|(a, _, _)| *a).collect();
    let term: Vec<i32> = combinations.iter().map(|(_, t, _)| *t).collect();
    let gender: Vec<&str> = combinations.iter().map(|(_, _, g)| *g).collect();
    // If more information on demographics is needed, it can be added here.
    let policy_count: Vec<f64> = vec![1.0; combinations.len()];
    let sum_insured: Vec<f64> = vec![1000.0; combinations.len()];

    let df = df![
        "model" => model,
        "id" => id,
        "age" => age,
        "term" => term,
        "gender" => gender,
        "policy_count" => policy_count,
        "sum_insured" => sum_insured,
    ]?;

    Ok(df)
}
