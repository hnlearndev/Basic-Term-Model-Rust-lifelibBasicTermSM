use super::*;

//---------------------------------------------------------------------------------------------------------
// STRUCT
//---------------------------------------------------------------------------------------------------------
pub struct SEModelPoint {
    pub model: String,
    pub id: i32,
    pub entry_age: i32,
    pub gender: String,
    pub term: i32,
    pub policy_count: f64,
    pub sum_insured: f64,
    pub duration_mth: i32,
}

impl SEModelPoint {
    pub fn project(&self, assumptions: &AssumptionScenario) -> PolarsResult<LazyFrame> {
        todo!("Implement SEModelPoint projection logic here");
    }
}

//---------------------------------------------------------------------------------------------------------
// PRIVATE
//---------------------------------------------------------------------------------------------------------
