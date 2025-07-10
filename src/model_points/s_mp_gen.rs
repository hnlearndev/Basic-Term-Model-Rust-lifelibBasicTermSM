use super::*;

pub fn generate_s_model_points(mp_size: usize, seed: usize) -> PolarsResult<DataFrame> {
    // Get seed for random number generation
    let mut rng = StdRng::seed_from_u64(seed as u64);

    // Issue Age (Integer): Random 20 - 59 year old
    let entry_age = Array1::random_using(mp_size, Uniform::new(20, 60), &mut rng); // 60 is exclusive, so range is 20-59

    // Gender (String): Random "M" and "F"
    let gender_binary = Array1::random_using(mp_size, Uniform::new(0, 2), &mut rng); // 0 or 1
    let gender: Vec<&str> = gender_binary
        .iter()
        .map(|&x| if x == 0 { "M" } else { "F" }) // map 0 to "M" and 1 to "F"
        .collect();

    // Policy term (Integer): Random 10, 15 or 20
    let term = (Array1::random_using(mp_size, Uniform::new(2, 5), &mut rng)) * 5;

    // Policy count
    let policy_count = Array1::<f64>::ones(mp_size);

    // Sum insured (Float): Random values between 100,000 and 1,000,000 (multiple of 1000)
    let sum_insured = Array1::random_using(mp_size, Uniform::new(0.0f64, 1.0f64), &mut rng) // Random floats between 0 and 1
        .mapv(|x| (((900_000.0 * x + 100_000.0) / 1000.0).round() * 1000.0));

    // Create a DataFrame with the generated data
    let model_points_df = df![
        "id"  => (1..(mp_size+1) as i32).collect::<Vec<i32>>(),
        "entry_age" => entry_age.to_vec(),
        "gender" => gender,
        "term" => term.to_vec(),
        "policy_count" => policy_count.to_vec().into_iter().collect::<Vec<f64>>(),
        "sum_insured" => sum_insured.to_vec(),
    ]?;

    Ok(model_points_df)
}
