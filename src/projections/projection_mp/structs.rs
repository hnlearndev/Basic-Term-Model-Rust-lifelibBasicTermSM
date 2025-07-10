use super::*;

//---------------------------------------------------------------------------------------------------------
// STRUCTS
//---------------------------------------------------------------------------------------------------------
pub struct SModelPoint {
    pub model: String,
    pub id: i32,
    pub entry_age: i32,
    pub gender: String,
    pub term: i32,
    pub policy_count: f64,
    pub sum_insured: f64,
}

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

pub struct ASLSEModelPoint {
    pub model: String,
    pub id: i32,
    pub entry_age: i32,
    pub gender: String,
    pub term: i32,
    pub policy_count: f64,
    pub sum_insured: f64,
    pub duration_mth: i32,
    pub issue_date: NaiveDate,
    pub payment_freq: i32,
    pub payment_term: i32,
}

pub enum ModelPoint {
    SModel(SModelPoint),
    SEModel(SEModelPoint),
    ASLSEModel(ASLSEModelPoint),
}
