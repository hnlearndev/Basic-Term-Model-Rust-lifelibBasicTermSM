use crate::projections::projection_mp::ModelPoint;
use ndarray::prelude::*;
use ndarray_rand::RandomExt;
use ndarray_rand::rand::SeedableRng;
use ndarray_rand::rand::rngs::StdRng;
use ndarray_rand::rand_distr::Uniform;
use polars::prelude::*;

pub mod asl_se_mp_gen;
pub mod pricing_mp_gen;
pub mod s_mp_gen;
pub mod se_mp_gen;

//---------------------------------------------------------------------------------------------------------
// PUBLIC
//---------------------------------------------------------------------------------------------------------
// pub fn convert_model_points_df_to_vector(df: &DataFrame) -> PolarsResult<Vec<ModelPoint>> {
//     let id = df.column("id")?.i32()?;
//     let entry_age = df.column("entry_age")?.i32()?;
//     let gender = df.column("gender")?.str()?;
//     let term = df.column("term")?.i32()?;
//     let policy_count = df.column("policy_count")?.f64()?;
//     let sum_insured = df.column("sum_insured")?.f64()?;
//     let model = df.column("model")?.str()?;

//     let model_points = (0..df.height())
//         .map(|i| ModelPoint {
//             id: id.get(i).unwrap(),
//             entry_age: entry_age.get(i).unwrap(),
//             gender: gender.get(i).unwrap().to_string(),
//             term: term.get(i).unwrap(),
//             policy_count: policy_count.get(i).unwrap(),
//             sum_insured: sum_insured.get(i).unwrap(),
//             model: model.get(i).unwrap().to_string(),
//         })
//         .collect();

//     Ok(model_points)
// }
